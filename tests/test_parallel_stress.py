"""Front spec for ParallelStress lane building over the execution engine."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pytest

from corecycler.engine import parallel as parallel_mod
from corecycler.engine.backends.base import StressConfig, StressResult
from corecycler.engine.parallel import ParallelStress
from corecycler.engine.scheduler import SchedulerConfig
from corecycler.engine.topology import CPUTopology, PhysicalCore


class RecordingBackend:
    name = "fake"

    def __init__(self) -> None:
        self.cleaned: list[tuple[Path, bool]] = []

    def cleanup(self, work_dir, *, preserve_on_error: bool = False) -> None:
        self.cleaned.append((Path(work_dir), preserve_on_error))


class ScriptedSupervisor:
    script: ClassVar[list] = []
    created: ClassVar[list] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        ScriptedSupervisor.created.append(self)

    def run(self, lanes, config_for, duration):
        step = ScriptedSupervisor.script.pop(0)
        return step(self, lanes, config_for, duration)


@pytest.fixture(autouse=True)
def scripted(monkeypatch):
    ScriptedSupervisor.script = []
    ScriptedSupervisor.created = []
    monkeypatch.setattr(parallel_mod, "Supervisor", ScriptedSupervisor)
    return ScriptedSupervisor


def make_topo(cores: dict[int, tuple[int, ...]] | None = None) -> CPUTopology:
    topo = CPUTopology()
    for core_id, cpus in (cores or {0: (16, 0), 1: (1, 17)}).items():
        topo.cores[core_id] = PhysicalCore(core_id=core_id, ccd=0, ccx=None, logical_cpus=cpus)
    return topo


def make_parallel(tmp_path, *, topo=None, backend=None, cores=None) -> ParallelStress:
    return ParallelStress(
        topology=topo or make_topo(),
        backend=backend or RecordingBackend(),
        stress_config=StressConfig(),
        scheduler_config=SchedulerConfig(seconds_per_core=1, poll_interval=0.01, cores_to_test=cores),
        work_dir=tmp_path / "work",
    )


def ok(core_id: int) -> StressResult:
    return StressResult(core_id=core_id, passed=True, duration_seconds=0.1)


def bad(core_id: int) -> StressResult:
    return StressResult(
        core_id=core_id,
        passed=False,
        duration_seconds=0.1,
        error_message="mprime error: FATAL ERROR",
        error_type="computation",
    )


class TestLaneBuilding:
    def test_lanes_cover_every_core_with_sorted_cpus(self, tmp_path):
        runner = make_parallel(tmp_path)
        runner.stress_config.threads = 2
        runner._requested_threads = 2
        seen: list[tuple[int, tuple[int, ...], int]] = []

        def capture(sup, lanes, config_for, duration):
            for one in lanes:
                seen.append((one.core_id, one.cpus, config_for(one).threads))
            return {one.core_id: ok(one.core_id) for one in lanes}

        ScriptedSupervisor.script = [capture]
        results = runner.run()
        assert ScriptedSupervisor.created[0].kwargs["stop_on_first_failure"] is True
        assert seen == [(0, (0, 16), 2), (1, (1, 17), 2)]
        assert sorted(results) == [0, 1]
        assert runner.work_dir.is_dir()

    def test_one_thread_lanes_use_one_logical_cpu_each(self, tmp_path):
        runner = make_parallel(tmp_path)
        seen: list[tuple[int, ...]] = []

        def capture(sup, lanes, config_for, duration):
            seen.extend(one.cpus for one in lanes)
            for one in lanes:
                config_for(one)
            return {one.core_id: ok(one.core_id) for one in lanes}

        ScriptedSupervisor.script = [capture]
        runner.run()
        assert seen == [(0,), (1,)]

    def test_a_core_outside_the_topology_fails_closed(self, tmp_path):
        runner = make_parallel(tmp_path, cores=[0, 9])
        results = runner.run()
        assert list(results) == [9]
        assert results[9].error_type == "startup"
        assert ScriptedSupervisor.created == []

    def test_an_empty_topology_returns_empty_not_crash(self, tmp_path):
        runner = make_parallel(tmp_path, topo=CPUTopology())
        assert runner.run() == {}

    def test_interrupted_lanes_stay_absent_from_the_results(self, tmp_path):
        runner = make_parallel(tmp_path)

        def one_fails_one_interrupted(sup, lanes, config_for, duration):
            return {0: bad(0), 1: None}

        ScriptedSupervisor.script = [one_fails_one_interrupted]
        results = runner.run()
        assert list(results) == [0]
        assert results[0].passed is False

    def test_cleanup_preserves_only_failed_lanes(self, tmp_path):
        backend = RecordingBackend()
        runner = make_parallel(tmp_path, backend=backend)
        ScriptedSupervisor.script = [lambda sup, lanes, c, d: {0: bad(0), 1: ok(1)}]
        runner.run()
        preserve = {path.name: flag for path, flag in backend.cleaned}
        assert preserve == {"core_0": True, "core_1": False}

    def test_a_reused_instance_starts_clean(self, tmp_path):
        runner = make_parallel(tmp_path)
        ScriptedSupervisor.script = [lambda sup, lanes, c, d: {0: ok(0), 1: ok(1)}]
        runner.stop()
        runner.observed_mce.append(object())
        results = runner.run()
        assert sorted(results) == [0, 1]
        assert runner.observed_mce == []

    def test_stop_sets_the_shared_event(self, tmp_path):
        runner = make_parallel(tmp_path)
        runner.stop()
        assert runner._stop_event.is_set()
        assert type(runner).force_stop is type(runner).stop

    def test_the_default_work_dir_is_per_user(self):
        from corecycler.config.paths import resolve_work_dir

        runner = ParallelStress(
            topology=make_topo(),
            backend=RecordingBackend(),
            stress_config=StressConfig(),
            scheduler_config=SchedulerConfig(),
        )
        assert runner.work_dir == resolve_work_dir()
        assert "/tmp/corecycler" not in str(runner.work_dir)
        assert not (Path(__file__).parent.parent / "core_0").exists()


@pytest.mark.parametrize("requested", [None, 99999, 512])
def test_memory_backend_shares_one_budget_at_launch(tmp_path, monkeypatch, requested):
    from types import SimpleNamespace

    backend = RecordingBackend()
    backend.name = "stressapptest"
    runner = make_parallel(tmp_path, backend=backend)
    runner.stress_config.memory_mb = requested
    calls = []
    budget = SimpleNamespace(
        per_instance_mb=2048, minimum_per_instance_mb=256, fits=True, describe=lambda: "two workers sharing 4096 MB"
    )
    monkeypatch.setattr(parallel_mod, "memory_budget", lambda n, minimum, **kw: calls.append(n) or budget)
    seen = []

    def capture(sup, lanes, config_for, duration):
        for lane in lanes:
            cfg = config_for(lane)
            # Verify the actual binary command always receives an explicit cap.
            from corecycler.engine.backends.stressapptest import StressapptestBackend

            real = StressapptestBackend()
            monkeypatch.setattr(real, "require_binary", lambda: "stressapptest")
            command = real.get_command(cfg, lane.work_dir)
            seen.append(int(command[command.index("-M") + 1]))
        return {lane.core_id: ok(lane.core_id) for lane in lanes}

    ScriptedSupervisor.script = [capture]
    runner.run()
    assert calls == [2]
    assert seen == [min(requested or 2048, 2048)] * 2
    assert runner.stress_config.memory_mb == requested


def test_unreadable_memory_budget_starts_no_workers(tmp_path, monkeypatch):
    from corecycler.engine.memory_budget import MemoryBudgetUnavailable

    backend = RecordingBackend()
    backend.name = "stressapptest"
    runner = make_parallel(tmp_path, backend=backend)

    def unavailable(*args, **kwargs):
        raise MemoryBudgetUnavailable("meminfo unavailable")

    monkeypatch.setattr(parallel_mod, "memory_budget", unavailable)
    results = runner.run()
    assert results[0].error_type == "startup"
    assert "meminfo unavailable" in results[0].error_message
    assert ScriptedSupervisor.created == []


def test_insufficient_memory_budget_starts_no_workers(tmp_path, monkeypatch):
    from types import SimpleNamespace

    backend = RecordingBackend()
    backend.name = "stressapptest"
    runner = make_parallel(tmp_path, backend=backend)
    budget = SimpleNamespace(fits=False, describe=lambda: "only 10 MB available per worker")
    monkeypatch.setattr(parallel_mod, "memory_budget", lambda *a, **kw: budget)
    results = runner.run()
    assert results[0].error_type == "startup"
    assert "10 MB" in results[0].error_message
    assert ScriptedSupervisor.created == []

import threading
import time
from threading import RLock
from typing import List, Dict, Tuple

import predictor
from cloud_controller import THROUGHPUT_ENABLED, THROUGHPUT_PERCENTILES, DEFAULT_HARDWARE_ID


class MultiPredictor:

    def __init__(self):
        self.lock = RLock()
        self.percentiles: List[float] = []
        if THROUGHPUT_ENABLED:
            self.percentiles.extend(THROUGHPUT_PERCENTILES)
        self.hw_ids: List[str] = [DEFAULT_HARDWARE_ID]
        self._predictors: Dict[Tuple[str, float], predictor.Predictor] = {}
        for hw_id in self.hw_ids:
            for percentile in self.percentiles:
                self._add_predictor(hw_id, percentile)

    def _add_predictor(self, hw_id: str, percentile:float) -> None:
        self._predictors[(hw_id, percentile)] = self._create_predictor(hw_id, percentile)

    def add_hw_id(self, hw_id: str):
        for percentile in self.percentiles:
            self._add_predictor(hw_id, percentile)

    def add_percentile(self, percentile: float):
        for hw_id in self.hw_ids:
            self._add_predictor(hw_id, percentile)

    def predict_time(self, hw_id: str, combination: List[str], time_limit: int, percentile: float) -> bool:
        with self.lock:
            verdict, _ = self._predictors[(hw_id, percentile)].predict_combination(comb=combination, time_limit=time_limit)
        return verdict is not None and verdict

    def predict_throughput(self, hw_id: str, combination: List[str], max_value: int) -> bool:
        assert THROUGHPUT_ENABLED and len(THROUGHPUT_PERCENTILES) > 0
        total_time = 0.0
        previous = 0.0
        time = None
        for percentile in THROUGHPUT_PERCENTILES:
            with self.lock:
                _, prediction = self._predictors[(hw_id, percentile)].predict_combination(comb=combination, time_limit=0)
            time = prediction.combined
            if time is None:
                return False
            total_time += (percentile - previous) * time
            previous = percentile
        total_time += (100 - previous) * time

        return total_time < max_value * 100 # we multiply it by 100 since the percentiles are 1 to 100, not 0 to 1.

    @staticmethod
    def _create_predictor(hw_id: str, percentile: float) -> predictor.Predictor:
        _predictor = predictor.Predictor(nodetype=hw_id, percentile=percentile)
        _predictor.assign_headers("headers.json")
        _predictor.assign_groundtruth("groundtruth.json")
        _predictor.assign_user_boundary("user_boundary.json")

        from clustering_alg import MeanShift
        from clustering_score import VMeasure
        from distance import AveragePairCorrelation
        from normalizer import MinMax
        from optimizer import SimAnnealing
        _predictor.configure(
            clustering_alg=MeanShift(),
            clustering_score=VMeasure(),
            distance=AveragePairCorrelation(),
            normalizer=MinMax(),
            optimizer=SimAnnealing(),
            boundary_percentage=140)
        return _predictor

    def provide_new_files(self, files: Dict[str, List[str]]) -> None:
        for ((hw_id, _), predictor) in self._predictors.items():
            if hw_id in files:
                for filename in files[hw_id]:
                    predictor.provide_data_matrix(filename)
                predictor.prepare_predictor()


class PredictorUpdater:

    def __init__(self, predictor_: MultiPredictor):
        self._files: Dict[str, List[str]] = {}
        self._update_time: bool = False
        self._predictor: MultiPredictor = predictor_
        self._file_count: int = 0
        self._lock = RLock()

    @property
    def file_count(self) -> int:
        return self._file_count

    def start(self) -> None:
        threading.Thread(target=self._run, args=(), daemon=True).start()

    def _run(self) -> None:
        if self._update_time:
            with self._lock:
                self._update_time = False
                files = self._files
                self._files = {}
                self._file_count = 0
            self._predictor.provide_new_files(files)

        else:
            time.sleep(1)

    def provide_file(self, hw_id: str, filename:str):
        with self._lock:
            if hw_id not in self._files:
                self._files[hw_id] = []
            self._files[hw_id].append(filename)
            self._file_count += 1

    def update_predictor(self) -> None:
        with self._lock:
            self._update_time = True
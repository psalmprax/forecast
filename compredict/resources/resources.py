from .base import BaseResource


class Algorithm(BaseResource):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_result = None

    def run(self, data, evaluate=True, encrypt=False):
        self._last_result = self.client.run_algorithm(self.id, data, evaluate, encrypt)
        return self.last_results

    def get_detailed_template(self, file_type='input'):
        return self.client.get_template(self.id, file_type)

    def get_detailed_graph(self, file_type='input'):
        return self.client.get_graph(self.id, file_type)

    def get_response_time(self):
        return self.result

    def get_template(self, file_type='input'):
        return self.features_output if file_type == 'output' else self.features_format

    @property
    def last_results(self):
        return self._last_result


class Evaluation(BaseResource):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Result(BaseResource):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Task(BaseResource):

    STATUS_PENDING = "Pending"
    STATUS_PROGRESS = "In Progress"
    STATUS_FINISHED = "Finished"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.status = self.status if self.status is not None else Task.STATUS_PENDING
        self.success = self.success if self.success is not None else None
        self.error = self.error if self.error is not None else None
        self.is_encrypted = self.is_encrypted if self.is_encrypted is not None else False
        self.set_results(self.predictions, self.evaluations)

    def update(self):
        task = self.client.get_task_results(self.job_id)
        self.__dict__.update(task.__dict__)

    def get_current_status(self):
        return self.status

    def set_results(self, predictions, evaluations):
        self.predictions = None
        self.evaluations = None
        if self.status == Task.STATUS_FINISHED and self.success is True:
            if self.is_encrypted:
                self.predictions = self.client.RSA_decrypt(predictions)
                if evaluations is not None:
                    self.evaluations = self.client.RSA_decrypt(evaluations)
            else:
                self.predictions = predictions
                self.evaluations = evaluations

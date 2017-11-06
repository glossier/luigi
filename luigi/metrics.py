class MetricsCollector(object):
    """
    Dummy MetricsCollecter base class that can be replace by tool specific
    implementation
    """
    def __init__(self, scheduler):
        self._scheduler = scheduler

    def handle_task_started(self, task):
        pass

    def handle_task_failed(self, task):
        pass

    def handle_task_disabled(self, task):
        pass

    def handle_task_done(self, task):
        pass

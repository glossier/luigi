from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config

from datadog import initialize, api


class datadog(Config):
    api_key = parameter.Parameter(default='dummy_api_key')
    app_key = parameter.Parameter(default='dummy_app_key')


class DataDogMetricsCollector(MetricsCollector):
    def __init__(self, *args, **kwargs):
        super(DataDogMetricsCollector, self).__init__(*args, **kwargs)
        self._config = datadog(**kwargs)
        initialize(api_key=self._config.api_key, app_key=self._config.app_key)

    def handle_task_started(self, task):
        self._add_event(task.family)

    def _add_event(self, task_family):
        title = "Did this work? -- I hope so!"
        text = 'And let me tell you all about it here!'
        tags = ['dw-test', 'version:1', 'application:web']
        api.Event.create(title=title, text=text, tags=tags)

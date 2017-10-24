from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config

from datadog import initialize, api


class datadog(Config):
    api_key = parameter.Parameter(default='dummy_api_key')
    app_key = parameter.Parameter(default='dummy_app_key')
    default_event_tags = parameter.Parameter(default=None)


class DataDogMetricsCollector(MetricsCollector):
    def __init__(self, *args, **kwargs):
        super(DataDogMetricsCollector, self).__init__(*args, **kwargs)
        self._config = datadog(**kwargs)
        initialize(api_key=self._config.api_key, app_key=self._config.app_key)

    def handle_task_started(self, task):
        title = "Luigi: A task has been started!"
        text = "A task has been started in the Pipeline named: {name}".format(name=task.family)
        tags = ["task_state:STARTED",
                "task_name:{name}".format(name=task.family)]

        self._add_event(title=title, text=text,
                        tags=tags, alert_type='info',
                        priority='low')

    def _add_event(self,
                   title=None, text=None,
                   tags=[], alert_type='info',
                   priority='normal'):

        all_tags = tags + self.default_event_tags()
        api.Event.create(title=title, text=text,
                         tags=all_tags, alert_type=alert_type,
                         priority=priority)

    def default_event_tags(self):
        if not self._config.default_event_tags:
            return []

        return str.split(self._config.default_event_tags, ',')

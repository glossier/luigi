from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config

from datadog import initialize, api, statsd


class datadog(Config):
    api_key = parameter.Parameter(default='dummy_api_key')
    app_key = parameter.Parameter(default='dummy_app_key')
    default_event_tags = parameter.Parameter(default=None)
    environment = parameter.Parameter(default='development', description='Environment of the pipeline')
    metric_namespace = parameter.Parameter(default='luigi')


class DataDogMetricsCollector(MetricsCollector):
    def __init__(self, *args, **kwargs):
        super(DataDogMetricsCollector, self).__init__(*args, **kwargs)
        self._config = datadog(**kwargs)
        initialize(api_key=self._config.api_key, app_key=self._config.app_key)

    def handle_task_started(self, task):
        title = "Luigi: A task has been started!"
        text = "A task has been started in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self.send_increment('task.started', tags=tags)

        event_tags = tags + ["task_state:STARTED"]
        self.send_event(title=title, text=text, tags=event_tags, alert_type='info', priority='low')

    def handle_task_failed(self, task):
        title = "Luigi: A task has failed!"
        text = "A task has failed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self.send_increment('task.failed', tags=tags)

        event_tags = tags + ["task_state:FAILED"]
        self.send_event(title=title, text=text, tags=event_tags, alert_type='error', priority='normal')

    def handle_task_disabled(self, task, config):
        title = "Luigi: A task has been disabled!"
        text = """A task has been disabled in the pipeline named: {name}.
                  The task has failed {failures} times in the last {window}
                  seconds, so it is being disabled for {persist} seconds.""".format(
                       name=task.family,
                       persist=config.disable_persist,
                       failures=task.retry_policy.retry_count,
                       window=config.disable_window
                       )
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self.send_increment('task.disabled', tags=tags)

        event_tags = tags + ["task_state:DISABLED"]
        self.send_event(title=title, text=text, tags=event_tags, alert_type='error', priority='normal')

    def handle_task_done(self, task):
        # The task is already done -- Let's not re-create an event
        if task.time_running is None:
            return

        title = "Luigi: A task has been completed!"
        text = "A task has completed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        time_elapse = task.updated - task.time_running

        self.send_increment('task.done', tags=tags)
        self.send_gauge('task.execution_time', time_elapse, tags=tags)

        event_tags = tags + ["task_state:DONE"]
        self.send_event(title=title, text=text, tags=event_tags, alert_type='info', priority='low')

    def send_event(self, title=None, text=None, tags=[], alert_type='info', priority='normal'):
        all_tags = tags + self.default_event_tags()

        api.Event.create(title=title, text=text, tags=all_tags, alert_type=alert_type, priority=priority)

    def send_gauge(self, metric_name, value, tags=[]):
        all_tags = tags + self.default_event_tags()

        namespaced_metric = "{namespace}.{metric_name}".format(namespace=self._config.metric_namespace,
                                                               metric_name=metric_name)
        statsd.gauge(namespaced_metric, value, tags=all_tags)

    def send_increment(self, metric_name, value=1, tags=[]):
        all_tags = tags + self.default_event_tags()

        namespaced_metric = "{namespace}.{metric_name}".format(namespace=self._config.metric_namespace,
                                                               metric_name=metric_name)
        statsd.increment(namespaced_metric, value, tags=all_tags)

    def _format_task_params_to_tags(self, task):
        params = []
        for key, value in task.params.items():
            params.append("{key}:{value}".format(key=key, value=value))

        return params

    def default_event_tags(self):
        default_tags = []

        if self._config.default_event_tags:
            default_tags = default_tags + str.split(self._config.default_event_tags, ',')

        if self._config.environment:
            env_tag = "env={environment}".format(environment=self._config.environment)
            default_tags.append(env_tag)

        return default_tags

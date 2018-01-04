from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config

from datadog import initialize, api, statsd


class datadog(Config):
    api_key = parameter.Parameter(default='dummy_api_key')
    app_key = parameter.Parameter(default='dummy_app_key')
    default_event_tags = parameter.Parameter(default=None)
    metric_namespace = parameter.Parameter(default='luigi')


class DataDogMetricsCollector(MetricsCollector):
    def __init__(self, *args, **kwargs):
        super(DataDogMetricsCollector, self).__init__(*args, **kwargs)
        self._config = datadog(**kwargs)
        initialize(api_key=self._config.api_key, app_key=self._config.app_key)

    def handle_task_started(self, task):
        title = "Luigi: A task has been started!"
        text = "A task has been started in the pipeline named: {name}".format(name=task.family)
        tags = ["task_state:STARTED",
                "task_name:{name}".format(name=task.family)]
        tags = tags + self._format_task_params_to_tags(task)

        statsd.increment('{namespace}.task.started'.format(namespace=self._config.metric_namespace))
        self._add_event(title=title, text=text,
                        tags=tags, alert_type='info',
                        priority='low')

    def handle_task_failed(self, task):
        title = "Luigi: A task has failed!"
        text = "A task has failed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_state:FAILED",
                "task_name:{name}".format(name=task.family)]
        tags = tags + self._format_task_params_to_tags(task)

        statsd.increment('{namespace}.task.failed'.format(namespace=self._config.metric_namespace))
        self._add_event(title=title, text=text,
                        tags=tags, alert_type='error',
                        priority='normal')

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

        tags = ["task_state:DISABLED",
                "task_name:{name}".format(name=task.family)]
        tags = tags + self._format_task_params_to_tags(task)

        statsd.increment('{namespace}.task.disabled'.format(namespace=self._config.metric_namespace))
        self._add_event(title=title, text=text,
                        tags=tags, alert_type='error',
                        priority='normal')

    def handle_task_done(self, task):
        title = "Luigi: A task has been completed!"
        text = "A task has completed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_state:DONE",
                "task_name:{name}".format(name=task.family)]
        tags = tags + self._format_task_params_to_tags(task)

        # The task is already done -- Let's not re-create an event
        if task.time_running is None:
            return

        time_elapse = task.updated - task.time_running

        statsd.increment('{namespace}.task.done'.format(namespace=self._config.metric_namespace))
        statsd.gauge('{namespace}.{name}.execution_time'.format(
            namespace=self._config.metric_namespace,
            name=task.family), time_elapse)
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

    def _format_task_params_to_tags(self, task):
        params = []
        for key, value in task.params.items():
            params.append("{key}:{value}".format(key=key, value=value))

        return params

    def default_event_tags(self):
        if not self._config.default_event_tags:
            return []

        return str.split(self._config.default_event_tags, ',')

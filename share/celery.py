import logging
import functools

import raven

from celery import states
from celery.app.task import Context
from celery.utils.time import maybe_timedelta
from celery.backends.base import BaseDictBackend

from django.utils import timezone
from django.conf import settings

from share.models import CeleryTaskResult

logger = logging.getLogger(__name__)


if hasattr(settings, 'RAVEN_CONFIG') and settings.RAVEN_CONFIG['dsn']:
    client = raven.Client(settings.RAVEN_CONFIG['dsn'])
else:
    client = None


def die_on_unhandled(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            try:
                logger.exception('Celery internal method %s failed', func)
                try:
                    if client:
                        client.capture_exception()
                except Exception as ee:
                    logger.exception('Could not log exception to Sentry')
            finally:
                raise SystemExit(57)  # Something a bit less generic than 1 or -1
    return wrapped


# Based on https://github.com/celery/django-celery-results/commit/f88c677d66ba1eaf1b7cb1f3b8c910012990984f
class CeleryDatabaseBackend(BaseDictBackend):
    TaskModel = CeleryTaskResult

    @die_on_unhandled
    def _store_result(self, task_id, result, status, traceback=None, request=None, **kwargs):
        fields = {
            'meta': {},
            'result': result,
            'traceback': traceback,
            'celery_meta': self.current_task_children(request),
        }

        if status is not None:
            fields['status'] = status

        if isinstance(result, dict):
            fields['meta'].update(result)

        if isinstance(request, Context):
            fields.update({
                'task_name': request.task,
                'correlation_id': request.correlation_id,
            })

            fields['meta'].update({
                'args': request.args,
                'kwargs': request.kwargs,
            })

        obj, created = self.TaskModel.objects.get_or_create(task_id=task_id, defaults=fields)

        if not created:
            for key, value in fields.items():
                if isinstance(value, dict) and getattr(obj, key, None):
                    getattr(obj, key).update(value)
                else:
                    setattr(obj, key, value)
            obj.save()

        return obj

    @die_on_unhandled
    def cleanup(self):
        self.TaskModel.objects.filter(date_modified=timezone.now() - maybe_timedelta(self.expires), status=states.SUCCESS).delete()

    @die_on_unhandled
    def _get_task_meta_for(self, task_id):
        return self.TaskModel.objects.get(task_id=task_id).as_dict()

    @die_on_unhandled
    def _forget(self, task_id):
        try:
            self.TaskModel._default_manager.get(task_id=task_id).delete()
        except self.TaskModel.DoesNotExist:
            pass

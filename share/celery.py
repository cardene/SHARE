import logging

from celery import Task
from celery import states
from celery.utils.time import maybe_timedelta

from celery.backends.base import BaseDictBackend
from django.utils import timezone

from share.models import CeleryTaskResult

logger = logging.getLogger(__name__)


class CeleryTask(Task):

    def share_annotate(self, data):
        if not hasattr(self.request, 'share_meta'):
            self.request.share_meta = {}
        self.request.share_meta.update(data)


# Based on https://github.com/celery/django-celery-results/commit/f88c677d66ba1eaf1b7cb1f3b8c910012990984f
class CeleryDatabaseBackend(BaseDictBackend):
    TaskModel = CeleryTaskResult

    def _store_result(self, task_id, result, status, traceback=None, request=None):
        fields = {
            'status': status,
            'result': result,
            'traceback': traceback,
            'celery_meta': self.current_task_children(request)
        }

        if request:
            fields.update({
                'task_name': request.task,
                'correlation_id': request.correlation_id,
                'meta': {
                    'args': request.args,
                    'kwargs': request.kwargs,
                    'share_meta': getattr(request, 'share_meta', {}),
                }
            })

        obj, created = self.TaskModel.objects.get_or_create(task_id=task_id, defaults=fields)

        if not created:
            for key, value in fields.items():
                setattr(obj, key, value)
            obj.save()

        return obj

    def _get_task_meta_for(self, task_id):
        return self.TaskModel.objects.get(task_id=task_id).as_dict()

    def _forget(self, task_id):
        try:
            self.TaskModel._default_manager.get(task_id=task_id).delete()
        except self.TaskModel.DoesNotExist:
            pass

    def cleanup(self):
        self.TaskModel.objects.filter(date_modified=timezone.now() - maybe_timedelta(self.expires), state=states.SUCCESS).delete()

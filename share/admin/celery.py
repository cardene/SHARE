from django.contrib import admin


class CeleryTaskResultAdmin(admin.ModelAdmin):
    list_display = ('task_id', 'task_name', 'status', 'date_modified', 'date_created', 'share_version')
    exclude = ('correlation_id', )
    readonly_fields = (
        'task_id',
        'task_name',
        'task_args', 'task_kwargs',
        'result', 'traceback',
        'date_created', 'date_modified',
        'share_version', 'share_meta',
    )

    def task_args(self, obj):
        return obj.meta['args']

    def task_kwargs(self, obj):
        return obj.meta['kwargs']

    def share_meta(self, obj):
        return obj.meta

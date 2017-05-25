import pprint

from celery import states

from django.contrib import admin

from project import celery_app


class TaskNameFilter(admin.SimpleListFilter):
    title = 'Task'
    parameter_name = 'task_name'

    def lookups(self, request, model_admin):
        return sorted((x, x) for x in celery_app.tasks.keys())

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(task_name=self.value())
        return queryset


class StatusFilter(admin.SimpleListFilter):
    title = 'Status'
    parameter_name = 'status'

    def lookups(self, request, model_admin):
        return sorted((x, x.title()) for x in states.ALL_STATES)

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(status=self.value().upper())
        return queryset


class CeleryTaskResultAdmin(admin.ModelAdmin):
    list_display = ('task_id', 'task_name', 'status_', 'date_modified', 'date_created', 'share_version')
    exclude = ('correlation_id', )
    actions = ('retry', )
    ordering = ('-date_modified', )
    list_filter = (TaskNameFilter, StatusFilter, )
    readonly_fields = (
        'task_id',
        'task_name',
        'task_args', 'task_kwargs',
        'result', 'traceback',
        'meta_',
        'date_created', 'date_modified',
        'share_version'
    )

    def task_args(self, obj):
        return obj.meta['args']

    def task_kwargs(self, obj):
        return pprint.pformat(obj.meta['kwargs'])

    def status_(self, obj):
        return obj.status.title()
    status_.short_description = 'Status'

    def meta_(self, obj):
        return pprint.pformat(obj.meta)
    status_.short_description = 'Meta'

    def retry(self, request, queryset):
        for task in queryset:
            celery_app.tasks[task.task_name].apply_async(
                task.meta.get('args', ()),
                task.meta.get('kwargs', {}),
                task_id=str(task.task_id)
            )
    retry.short_description = 'Retry Tasks'

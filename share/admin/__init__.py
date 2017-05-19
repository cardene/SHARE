import datetime
from furl import furl

import celery

from django import forms
from django.apps import apps
from django.conf.urls import url
from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.admin.widgets import AdminDateWidget
from django.core import management
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html

from oauth2_provider.models import AccessToken

from share import tasks
from share.admin.celery import CeleryTaskResultAdmin
from share.admin.readonly import ReadOnlyAdmin
from share.admin.share_objects import CreativeWorkAdmin
from share.models.banner import SiteBanner
from share.models.celery import CeleryTaskResult
from share.models.change import ChangeSet
from share.models.core import NormalizedData, ShareUser
from share.models.creative import AbstractCreativeWork
from share.models.ingest import RawDatum, Source, SourceConfig, Harvester, Transformer
from share.models.logs import HarvestLog
from share.models.registration import ProviderRegistration
from share.robot import RobotAppConfig


admin.site.register(AbstractCreativeWork, CreativeWorkAdmin)
admin.site.register(CeleryTaskResult, CeleryTaskResultAdmin)


class NormalizedDataAdmin(admin.ModelAdmin):
    date_hierarchy = 'created_at'
    list_filter = ['source', ]
    raw_id_fields = ('raw', 'tasks',)


class ChangeSetSubmittedByFilter(SimpleListFilter):
    title = 'Source'
    parameter_name = 'source_id'

    def lookups(self, request, model_admin):
        return ShareUser.objects.filter(is_active=True).values_list('id', 'username')

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(normalized_data__source_id=self.value())
        return queryset


class ChangeSetAdmin(admin.ModelAdmin):
    list_display = ('status_', 'count_changes', 'submitted_by', 'submitted_at')
    actions = ['accept_changes']
    list_filter = ['status', ChangeSetSubmittedByFilter]
    raw_id_fields = ('normalized_data',)

    def submitted_by(self, obj):
        return obj.normalized_data.source
    submitted_by.short_description = 'submitted by'

    def count_changes(self, obj):
        return obj.changes.count()
    count_changes.short_description = 'number of changes'

    def status_(self, obj):
        return ChangeSet.STATUS[obj.status].title()


class AppLabelFilter(admin.SimpleListFilter):
    title = 'App Label'
    parameter_name = 'app_label'

    def lookups(self, request, model_admin):
        return sorted([
            (config.label, config.label)
            for config in apps.get_app_configs()
            if isinstance(config, RobotAppConfig)
        ])

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(app_label=self.value())
        return queryset


class TaskNameFilter(admin.SimpleListFilter):
    title = 'Task'
    parameter_name = 'task'

    def lookups(self, request, model_admin):
        return sorted(
            (key, key)
            for key in celery.current_app.tasks.keys()
            if key.startswith('share.')
        )

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(name=self.value())
        return queryset


class RawDatumAdmin(admin.ModelAdmin):
    raw_id_fields = ()


class AccessTokenAdmin(admin.ModelAdmin):
    list_display = ('token', 'user', 'scope')


class ProviderRegistrationAdmin(ReadOnlyAdmin):
    list_display = ('source_name', 'status_', 'submitted_at', 'submitted_by', 'direct_source')
    list_filter = ('direct_source', 'status',)
    readonly_fields = ('submitted_at', 'submitted_by',)

    def status_(self, obj):
        return ProviderRegistration.STATUS[obj.status].title()


class SiteBannerAdmin(admin.ModelAdmin):
    list_display = ('title', 'color', 'icon', 'active')
    list_editable = ('active',)
    ordering = ('-active', '-last_modified_at')
    readonly_fields = ('created_at', 'created_by', 'last_modified_at', 'last_modified_by')

    def save_model(self, request, obj, form, change):
        if not change:
            obj.created_by = request.user
        obj.last_modified_by = request.user
        super().save_model(request, obj, form, change)


class SourceConfigFilter(admin.SimpleListFilter):
    title = 'Source Config'
    parameter_name = 'source_config'

    def lookups(self, request, model_admin):
        # TODO make this into a cool hierarchy deal
        # return SourceConfig.objects.select_related('source').values_list('
        return SourceConfig.objects.order_by('label').values_list('id', 'label')

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(source_config=self.value())


class HarvestLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'source', 'label', 'share_version', 'status_', 'start_date_', 'end_date_', 'harvest_log_actions', )
    list_filter = ('status', SourceConfigFilter, )
    list_select_related = ('source_config__source', )
    readonly_fields = ('harvest_log_actions',)
    actions = ('restart_tasks', )

    STATUS_COLORS = {
        HarvestLog.STATUS.created: 'blue',
        HarvestLog.STATUS.started: 'cyan',
        HarvestLog.STATUS.failed: 'red',
        HarvestLog.STATUS.succeeded: 'green',
        HarvestLog.STATUS.rescheduled: 'goldenrod',
        HarvestLog.STATUS.forced: 'maroon',
        HarvestLog.STATUS.skipped: 'orange',
        HarvestLog.STATUS.retried: 'darkseagreen',
        HarvestLog.STATUS.cancelled: 'grey',
    }

    def source(self, obj):
        return obj.source_config.source.long_title

    def label(self, obj):
        return obj.source_config.label

    def start_date_(self, obj):
        return obj.start_date.isoformat()

    def end_date_(self, obj):
        return obj.end_date.isoformat()

    def status_(self, obj):
        if obj.status == HarvestLog.STATUS.created and (timezone.now() - obj.date_modified) > datetime.timedelta(days=1, hours=6):
            return format_html('<span style="font-weight: bold;">Lost</span>')
        return format_html(
            '<span style="font-weight: bold; color: {}">{}</span>',
            self.STATUS_COLORS[obj.status],
            HarvestLog.STATUS[obj.status].title(),
        )

    def restart_tasks(self, request, queryset):
        for log in queryset.select_related('source_config'):
            tasks.harvest.apply_async({'log_id': log.id}, task_id=log.task_id)
    restart_tasks.short_description = 'Restart selected tasks'

    def harvest_log_actions(self, obj):
        url = furl(reverse('admin:source-config-harvest', args=[obj.source_config_id]))
        url.args['start'] = self.start_date_(obj)
        url.args['end'] = self.end_date_(obj)
        url.args['superfluous'] = True
        return format_html('<a class="button" href="{}">Restart</a>', url.url)
    harvest_log_actions.short_description = 'Actions'


class HarvestForm(forms.Form):
    start = forms.DateField(widget=AdminDateWidget())
    end = forms.DateField(widget=AdminDateWidget())
    superfluous = forms.BooleanField(required=False)

    def clean(self):
        super().clean()
        if self.cleaned_data['start'] > self.cleaned_data['end']:
            raise forms.ValidationError('Start date cannot be after end date.')


class SourceConfigAdmin(admin.ModelAdmin):
    list_display = ('label', 'source_', 'version', 'enabled', 'source_config_actions')
    list_select_related = ('source',)
    readonly_fields = ('source_config_actions',)
    search_fields = ['label', 'source__name', 'source__long_title']

    def source_(self, obj):
        return obj.source.long_title

    def enabled(self, obj):
        return not obj.disabled
    enabled.boolean = True

    def get_urls(self):
        return [
            url(
                r'^(?P<config_id>.+)/harvest/$',
                self.admin_site.admin_view(self.harvest),
                name='source-config-harvest'
            )
        ] + super().get_urls()

    def source_config_actions(self, obj):
        if obj.harvester_id is None:
            return ''
        return format_html(
            '<a class="button" href="{}">Harvest</a>',
            reverse('admin:source-config-harvest', args=[obj.pk]),
        )
    source_config_actions.short_description = 'Actions'

    def harvest(self, request, config_id):
        config = self.get_object(request, config_id)
        if config.harvester_id is None:
            raise ValueError('You need a harvester to harvest.')

        if request.method == 'POST':
            form = HarvestForm(request.POST)
            if form.is_valid():
                kwargs = {
                    'start': form.cleaned_data['start'],
                    'end': form.cleaned_data['end'],
                    'superfluous': form.cleaned_data['superfluous'],
                    'async': True,
                    'quiet': True,
                    'ignore_disabled': True,
                }
                management.call_command('fullharvest', config.label, **kwargs)
                self.message_user(request, 'Started harvesting {}!'.format(config.label))
                url = reverse(
                    'admin:share_harvestlog_changelist',
                    current_app=self.admin_site.name,
                )
                return HttpResponseRedirect(url)
        else:
            initial = {'start': config.earliest_date, 'end': timezone.now().date()}
            for field in HarvestForm.base_fields.keys():
                if field in request.GET:
                    initial[field] = request.GET[field]
            form = HarvestForm(initial=initial)

        context = self.admin_site.each_context(request)
        context['opts'] = self.model._meta
        context['form'] = form
        context['source_config'] = config
        context['title'] = 'Harvest {}'.format(config.label)
        return TemplateResponse(request, 'admin/harvest.html', context)


class SourceAdminInline(admin.StackedInline):
    model = Source


class ShareUserAdmin(admin.ModelAdmin):
    inlines = (SourceAdminInline,)


class SourceAddForm(forms.ModelForm):

    title = forms.CharField(min_length=3, max_length=255, help_text='What this source will be displayed as to the end user. Must be unique.')
    url = forms.URLField(min_length=11, max_length=255, help_text='The home page or canonical URL for this source, make sure it is unique!.\nThe reverse DNS notation prepended with "sources." will be used to create a user for this source. IE share.osf.io -> sources.io.osf.share')

    class Meta:
        model = Source
        exclude = ('access_token', )
        fields = ('title', 'url', 'icon', )

    def validate_unique(self):
        # Forces validation checks on every field
        try:
            self.instance.validate_unique()
        except forms.ValidationError as e:
            # Translate field names because I'm a bad person
            if 'long_title' in e.error_dict:
                e.error_dict['title'] = e.error_dict.pop('long_title')
            if 'name' in e.error_dict:
                e.error_dict['url'] = e.error_dict.pop('name')
            if 'home_page' in e.error_dict:
                e.error_dict['url'] = e.error_dict.pop('home_page')
            self._update_errors(e)

    def _post_clean(self):
        if not self._errors:
            self.instance.home_page = self.cleaned_data['url'].lower().strip('/')
            self.instance.long_title = self.cleaned_data.pop('title')
            self.instance.name = '.'.join(reversed(self.instance.home_page.split('//')[1].split('.')))
        return super()._post_clean()

    def save(self, commit=True):
        instance = super().save(commit=False)

        user = ShareUser.objects.create_user(username='sources.' + instance.name, save=commit)
        user.set_unusable_password()
        instance.user = user

        if commit:
            instance.save()

        return instance


class SourceAdmin(admin.ModelAdmin):
    search_fields = ('name', 'long_title')
    readonly_fields = ('access_token', )

    def add_view(self, *args, **kwargs):
        self.form = SourceAddForm
        return super().add_view(*args, **kwargs)

    def save_model(self, request, obj, form, change):
        if obj.user and not (obj.user_id or obj.user.id):
            obj.user.save()
            obj.user_id = obj.user.id  # Django is weird
        obj.save()

    def access_token(self, obj):
        tokens = obj.user.accesstoken_set.all()
        if tokens:
            return tokens[0].token
        return None


admin.site.unregister(AccessToken)
admin.site.register(AccessToken, AccessTokenAdmin)

admin.site.register(ChangeSet, ChangeSetAdmin)
admin.site.register(HarvestLog, HarvestLogAdmin)
admin.site.register(NormalizedData, NormalizedDataAdmin)
admin.site.register(ProviderRegistration, ProviderRegistrationAdmin)
admin.site.register(RawDatum, RawDatumAdmin)
admin.site.register(SiteBanner, SiteBannerAdmin)

admin.site.register(Harvester)
admin.site.register(ShareUser, ShareUserAdmin)
admin.site.register(Source, SourceAdmin)
admin.site.register(SourceConfig, SourceConfigAdmin)
admin.site.register(Transformer)

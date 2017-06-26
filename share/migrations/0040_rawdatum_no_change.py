# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2017-06-26 20:09
from __future__ import unicode_literals

from django.db import migrations, models
import share.models.indexes


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0039_auto_20170614_1825'),
    ]

    operations = [
        migrations.AddField(
            model_name='rawdatum',
            name='no_change',
            field=models.NullBooleanField(help_text='Indicates that this RawDatum does not contain any new information. This allows the RawDataJanitor to find records that have not been processed.Records that do not contain changes will not have a NormalizedData associated with them, which would otherwise look like data that has not yet been processed.'),
        ),
    ]

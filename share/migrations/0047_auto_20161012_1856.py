# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2016-10-12 18:56
from __future__ import unicode_literals

from django.db import migrations
import share.models.fields
import share.models.validators


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0046_source_dedup'),
    ]

    operations = [
        migrations.RenameField(
            model_name='normalizeddata',
            old_name='normalized_data',
            new_name='data'
        )
    ]

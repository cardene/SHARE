# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2016-09-06 16:18
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0040_auto_20160909_1910'),
    ]

    operations = [
        migrations.AddField(
            model_name='shareuser',
            name='is_trusted',
            field=models.BooleanField(default=False, help_text='Designates whether the user can push directly into the db.', verbose_name='trusted'),
        ),
    ]

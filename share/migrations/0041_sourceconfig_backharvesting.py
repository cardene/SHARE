# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2017-05-16 20:13
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0040_auto_20170512_1442'),
    ]

    operations = [
        migrations.AddField(
            model_name='sourceconfig',
            name='backharvesting',
            field=models.BooleanField(default=False),
        ),
    ]

# Generated by Django 3.2.18 on 2023-03-17 20:13

import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0010_auto_20230306_1329"),
    ]

    operations = [
        migrations.CreateModel(
            name="ActivationInstanceEvent",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4, primary_key=True, serialize=False
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("event_number", models.IntegerField()),
                ("event_chunk_start_line", models.BigIntegerField()),
                ("event_chunk_end_line", models.BigIntegerField()),
                ("event_chunk", models.TextField(default="")),
                (
                    "activation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="core.activation",
                    ),
                ),
                (
                    "activation_instance",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="core.activationinstance",
                    ),
                ),
            ],
            options={
                "db_table": "core_activation_instance_event",
            },
        ),
        migrations.AddIndex(
            model_name="activationinstanceevent",
            index=models.Index(
                fields=["activation_instance", "event_number"],
                name="ix_act_activation_event",
            ),
        ),
        migrations.AddIndex(
            model_name="activationinstanceevent",
            index=models.Index(
                fields=["event_chunk_start_line", "event_chunk_end_line"],
                name="ix_act_event_start_end_line",
            ),
        ),
    ]

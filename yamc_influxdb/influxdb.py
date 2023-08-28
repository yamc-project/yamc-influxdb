# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import logging
import json

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError
from yamc.writers import Writer, HealthCheckException

from yamc.utils import Map, is_number


class InfluxDBWriter(Writer):
    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.host = self.config.value_str("host", regex="[a-zA-Z0-9\_\-\.]+")
        self.port = self.config.value_int("port", min=1, max=65535)
        self.user = self.config.value_str("user", default="")
        self.pswd = self.config.value_str("password", default="")
        self.dbname = self.config.value_str("dbname")
        self.max_body_size = self.config.value_str("max-body-size", default=20000000)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self.log.info(
                "Creating client connection, host=%s, port=%s, user=%s, password=(secret), dbname=%s"
                % (self.host, self.port, self.user, self.dbname)
            )
            self._client = InfluxDBClient(self.host, self.port, self.user, self.pswd, self.dbname)
        return self._client

    def healthcheck(self):
        try:
            super().healthcheck()
            self.client.ping()
        except Exception as e:
            self._client = None
            raise e

    def _create_fields_tags(self, data):
        def _value(v):
            if callable(getattr(v, "eval", None)):
                return v.eval(self.base_scope(Map(data=Map(data))))
            else:
                return v

        fields, tags = {}, {}
        for tag, value in data.get("tags", {}).items():
            tags[tag] = _value(value)
        for field, value in data.get("fields", {}).items():
            fields[field] = _value(value)
        return fields, tags

    def do_write(self, items):
        try:
            points = []
            size = 0
            for item in items:
                fields, tags = self._create_fields_tags(item.data)
                point = Map(
                    measurement=item.data.get("measurement", item.collector_id),
                    time=int(item.data.get("time", 0)) * 1000000000,
                    fields=fields,
                    tags=tags,
                )
                if point.time == 0:
                    self.log.error(
                        "Cannot write the data point %s to the influxdb due to a missing time field!" % str(point)
                    )
                    continue
                if len(point.fields.keys()) == 0:
                    self.log.warn("There are no fields in the data point %s!" % str(point))
                points.append(point)
                size += json.dumps(point).__sizeof__()
                if size > self.max_body_size:
                    self.log.debug(f"Writing the points as the size exceeded {self.max_body_size} bytes.")
                    self.client.write_points(points)
                    points = []
                    size = 0

            if len(points) > 0:
                self.client.write_points(points)
        except InfluxDBServerError as e:
            raise HealthCheckException("Writing of the points to influxdb failed!", e)
        except Exception as e:
            raise Exception(f"Writing the points to influxdb failed! (collector={item.collector_id})", e)

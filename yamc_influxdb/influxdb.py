# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import logging
import json

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError
from yamc.writers import Writer, HealthCheckException

from yamc.utils import Map, is_number

# time precision constants; the first number is the multiplier to convert the time to seconds, the second number is
# the multiplier to detect whether the time is in the given precision
TIME_PRECISION = [{"ns": (10**9, 10**18)}, {"us": (10**6, 10**15)}, {"ms": (10**3, 10**12)}, {"s": (1, 1)}]


def _time_precision(epoch_time):
    magnitude = abs(epoch_time)
    for tp in TIME_PRECISION:
        k, v = list(tp.items())[0]
        if magnitude > v[1]:
            return k, v
    return None, None


def _time_precision_item(precision):
    for tp in TIME_PRECISION:
        k, v = list(tp.items())[0]
        if k == precision:
            return v
    return None


class InfluxDBWriter(Writer):
    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.host = self.config.value_str("host", regex="[a-zA-Z0-9\_\-\.]+")
        self.port = self.config.value_int("port", min=1, max=65535)
        self.user = self.config.value_str("user", default="")
        self.pswd = self.config.value_str("password", default="")
        self.dbname = self.config.value_str("dbname")
        self.tp_label = self.config.value_str("time_precision", default="ms", regex="^(s|ms|us|ns)$")
        self.tp = _time_precision_item(self.tp_label)
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
                _time = item.data.get("time", 0)
                tp_p, tp_c = _time_precision(_time)
                if tp_p != self.tp_label:
                    _time = int((_time / tp_c[0]) * self.tp[0])
                else:
                    _time = int(_time)
                point = Map(
                    measurement=item.data.get("measurement", item.collector_id),
                    time=_time,
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
                # self.log.debug("Adding the data point %s to the influxdb." % str(point))
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

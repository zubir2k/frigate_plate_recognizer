"""MQTT messaging helpers."""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, Optional

import paho.mqtt.client as mqtt

from .metrics import (
    mqtt_sends_counter,
    on_connect_counter,
    on_disconnect_counter,
)

DISCOVERY_PREFIX = "homeassistant"
DEVICE_ID = "frigate_plate_recognizer"
STATE_TOPIC = "frigate/plate_recognizer"
IMAGE_TOPIC = "frigate/plate_recognizer/image"
PLACEHOLDER_IMAGE_PATH = "/app/placeholder.jpg"  # shipped with the container


def publish_discovery(client, logger) -> None:
    """Publish MQTT device discovery so HA auto-creates all entities."""
    device = {
        "identifiers": [DEVICE_ID],
        "name": "platerecognizer",
        "model": "frigate_plate_recognizer",
        "manufacturer": "Frigate / zubir2k",
    }
    origin = {
        "name": "frigate_plate_recognizer",
        "url": "https://github.com/zubir2k/frigate_plate_recognizer",
    }

    components = {
        "plate": {
            "p": "sensor",
            "name": "License Plate",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.plate_number }}",
            "json_attributes_topic": STATE_TOPIC,
            "json_attributes_template": "{{ value_json | tojson }}",
            "icon": "mdi:car",
            "unique_id": "platerecognizer_licenseplate",
            "default_entity_id": "sensor.platerecognizer_licenseplate",
        },
        "is_watched_plate": {
            "p": "binary_sensor",
            "name": "License Plate",
            "state_topic": STATE_TOPIC,
            "value_template": "{{ value_json.is_watched_plate }}",
            "payload_on": "True",
            "payload_off": "False",
            "icon": "mdi:eye-check",
            "unique_id": "platerecognizer_licenseplate_watched",
            "default_entity_id": "binary_sensor.platerecognizer_licenseplate",
        },
        "plate_image": {
            "p": "image",
            "name": "License Plate",
            "image_topic": IMAGE_TOPIC,
            "content_type": "image/jpeg",
            "icon": "mdi:image",
            "unique_id": "platerecognizer_licenseplate_image",
            "default_entity_id": "image.platerecognizer_licenseplate",
        },
    }

    payload = {
        "dev": device,
        "o": origin,
        "cmps": components,
    }

    discovery_topic = f"{DISCOVERY_PREFIX}/device/{DEVICE_ID}/config"
    client.publish(discovery_topic, json.dumps(payload), retain=True)
    logger.info("Published MQTT discovery to %s", discovery_topic)


def make_on_connect(
    logger, config: Dict[str, Any], on_connected: Optional[Callable] = None
) -> Callable:
    def _on_connect(client, userdata, flags, reason_code, properties):
        on_connect_counter.inc()
        logger.info("MQTT Connected")
        client.subscribe(config["frigate"]["main_topic"] + "/events")
        publish_discovery(client, logger)
        # Publish placeholder so the image entity is never NULL after HA restart
        try:
            with open(PLACEHOLDER_IMAGE_PATH, "rb") as f:
                client.publish(IMAGE_TOPIC, f.read(), retain=True)
                logger.debug("Published placeholder image to %s", IMAGE_TOPIC)
        except FileNotFoundError:
            logger.warning("Placeholder image not found at %s", PLACEHOLDER_IMAGE_PATH)
        if on_connected:
            on_connected(True)

    return _on_connect


def make_on_disconnect(
    logger, should_stop: Callable[[], bool], on_connected: Optional[Callable] = None
) -> Callable:
    def _on_disconnect(client, userdata, flags, reason_code, properties):
        on_disconnect_counter.inc()
        if on_connected:
            on_connected(False)

        if reason_code == 0:
            logger.info("MQTT disconnected cleanly")
            return

        logger.warning(
            "Unexpected MQTT disconnection (userdata:%s, flags:%s, properties:%s); reconnecting",
            userdata,
            flags,
            properties,
        )

        backoff_seconds = 5
        while not should_stop():  # pragma: no cover - backoff loop timing is hard to test
            try:
                client.reconnect()
                logger.info("MQTT reconnected")
                if on_connected:
                    on_connected(True)
                return
            except Exception as exc:
                logger.warning(
                    "MQTT reconnection failed (%s); retrying in %s seconds",
                    exc,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)

    return _on_disconnect


def publish_plate_message(
    *,
    mqtt_client,
    config: Dict[str, Any],
    plate_number: Optional[str],
    plate_score: Optional[float],
    frigate_event_id: str,
    after_data: Dict[str, Any],
    formatted_start_time: str,
    watched_plate: Optional[str],
    fuzzy_score: Optional[float],
    logger,
    snapshot: Optional[bytes] = None,
) -> None:
    if not config["frigate"].get("return_topic"):
        return

    mqtt_sends_counter.labels(watched=bool(watched_plate)).inc()

    if watched_plate:
        message = {
            "plate_number": str(watched_plate).upper(),
            "score": plate_score,
            "frigate_event_id": frigate_event_id,
            "camera_name": after_data["camera"],
            "start_time": formatted_start_time,
            "fuzzy_score": fuzzy_score,
            "original_plate": str(plate_number).upper(),
            "is_watched_plate": True,
        }
    else:
        message = {
            "plate_number": str(plate_number).upper() if plate_number else None,
            "score": plate_score,
            "frigate_event_id": frigate_event_id,
            "camera_name": after_data["camera"],
            "start_time": formatted_start_time,
            "is_watched_plate": False,
        }

    logger.debug("Sending MQTT message: %s", message)

    main_topic = config["frigate"]["main_topic"]
    return_topic = config["frigate"]["return_topic"]
    topic = f"{main_topic}/{return_topic}"

    mqtt_client.publish(topic, json.dumps(message), retain=True)

    # Publish cropped image bytes directly to the image topic for HA image entity
    if snapshot:
        mqtt_client.publish(IMAGE_TOPIC, snapshot, retain=True)
        logger.debug("Published cropped image to %s", IMAGE_TOPIC)


def create_mqtt_client(
    *,
    config: Dict[str, Any],
    logger,
    message_callback,
    on_connected: Optional[Callable] = None,
    should_stop: Callable[[], bool] | None = None,
) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.enable_logger()
    client.on_connect = make_on_connect(logger, config, on_connected)
    client.on_disconnect = make_on_disconnect(logger, should_stop or (lambda: False), on_connected)
    client.on_message = message_callback

    if config["frigate"].get("mqtt_username"):
        username = config["frigate"]["mqtt_username"]
        password = config["frigate"].get("mqtt_password", "")
        client.username_pw_set(username, password)

    return client


__all__ = ["publish_plate_message", "create_mqtt_client", "make_on_connect", "make_on_disconnect"]

"""Smoke test for the integration fixtures themselves — runs first to
catch container setup problems with clear failure messages, before the
real tests try to use the fixtures."""


def test_mosquitto_accepts_connections(mosquitto_container):
    import paho.mqtt.client as mqtt

    received = []

    def on_message(client, userdata, msg):
        received.append((msg.topic, msg.payload))

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id="smoke-sub")
    sub.on_message = on_message
    sub.connect(mosquitto_container.host, mosquitto_container.port)
    sub.subscribe("smoke/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id="smoke-pub")
    pub.connect(mosquitto_container.host, mosquitto_container.port)
    pub.publish("smoke/test", b"hello", qos=1).wait_for_publish(timeout=5)
    pub.disconnect()

    import time as _t
    for _ in range(50):
        if received:
            break
        _t.sleep(0.05)
    sub.loop_stop()
    sub.disconnect()

    assert received, "subscriber did not receive published message"
    assert received[0] == ("smoke/test", b"hello")


def test_mariadb_accepts_admin_connection(mariadb):
    with mariadb.admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
    assert "Maria" in version or "Maria".upper() in version.upper() \
        or "mariadb" in version.lower(), \
        f"unexpected version string: {version!r}"


def test_mariadb_truncate_clears_table(mariadb):
    with mariadb.admin_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO mqtt_events "
            "(timestamp, topic, payload, qos, retained) "
            "VALUES (NOW(6), 'smoke/topic', 'p', 0, 0)"
        )
        cur.execute("SELECT COUNT(*) FROM mqtt_events")
        assert cur.fetchone()[0] == 1

    mariadb.truncate()

    with mariadb.admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mqtt_events")
        assert cur.fetchone()[0] == 0

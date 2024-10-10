producer_config = {
    'bootstrap.servers': 'localhost:19092',
    'acks': '0',
    # 'security.protocol': 'SASL_SSL',
    # 'security.protocol': 'SASL_PLAINTEXT',
    # 'sasl.mechanisms': 'PLAIN',
    # 'sasl.username': 'anuj_walia@outlook.com',
    # 'sasl.password': 'Test@111',
    # 'sasl.jaas.config':"org.apache.kafka.common.security.plain.PlainLoginModule required username='anuj_walia@outlook.com' password='Test@111';"
}
sr_config = {
    'url': 'http://0.0.0.0:18081',
    'basic.auth.user.info':'anuj_walia@outlook.com:Test@111'
}


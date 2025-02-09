producer_config = {
    'bootstrap.servers': 'localhost:19092',
    'acks': '0',
    # 'security.protocol': 'SASL_SSL',
    # 'security.protocol': 'SASL_PLAINTEXT',
    # 'sasl.mechanisms': 'PLAIN',
    # 'sasl.username': 'user',
    # 'sasl.password': 'pass',
    # 'sasl.jaas.config':"org.apache.kafka.common.security.plain.PlainLoginModule required username='user password='pass';"
}
sr_config = {
    'url': 'http://0.0.0.0:18081',
    'basic.auth.user.info':'user:pass'
}


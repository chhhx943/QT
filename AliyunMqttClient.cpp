#include "AliyunMqttClient.h"

#include <QDateTime>
#include <QJsonDocument>
#include <QJsonValue>
#include <QMessageAuthenticationCode>
#include <QSslCertificate>
#include <QDebug>

namespace {

QSslConfiguration aliyunSslConfiguration()
{
    static const QByteArray aliyunIotRootCaPem =
        "-----BEGIN CERTIFICATE-----\n"
        "MIID3zCCAsegAwIBAgISfiX6mTa5RMUTGSC3rQhnestIMA0GCSqGSIb3DQEBCwUA\n"
        "MHcxCzAJBgNVBAYTAkNOMREwDwYDVQQIDAhaaGVqaWFuZzERMA8GA1UEBwwISGFu\n"
        "Z3pob3UxEzARBgNVBAoMCkFsaXl1biBJb1QxEDAOBgNVBAsMB1Jvb3QgQ0ExGzAZ\n"
        "BgNVBAMMEkFsaXl1biBJb1QgUm9vdCBDQTAgFw0yMzA3MDQwNjM2NThaGA8yMDUz\n"
        "MDcwNDA2MzY1OFowdzELMAkGA1UEBhMCQ04xETAPBgNVBAgMCFpoZWppYW5nMREw\n"
        "DwYDVQQHDAhIYW5nemhvdTETMBEGA1UECgwKQWxpeXVuIElvVDEQMA4GA1UECwwH\n"
        "Um9vdCBDQTEbMBkGA1UEAwwSQWxpeXVuIElvVCBSb290IENBMIIBIjANBgkqhkiG\n"
        "9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoK//6vc2oXhnvJD7BVhj6grj7PMlN2N4iNH4\n"
        "GBmLmMdkF1z9eQLjksYc4Zid/FX67ypWFtdycOei5ec0X00m53Gvy4zLGBo2uKgi\n"
        "T9IxMudmt95bORZbaph4VK82gPNU4ewbiI1q2loRZEHRdyPORTPpvNLHu8DrYBnY\n"
        "Vg5feEYLLyhxg5M1UTrT/30RggHpaa0BYIPxwsKyylQ1OskOsyZQeOyPe8t8r2D4\n"
        "RBpUGc5ix4j537HYTKSyK3Hv57R7w1NzKtXoOioDOm+YySsz9sTLFajZkUcQci4X\n"
        "aedyEeguDLAIUKiYicJhRCZWljVlZActorTgjCY4zRajodThrQIDAQABo2MwYTAO\n"
        "BgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUkWHoKi2h\n"
        "DlS1/rYpcT/Ue+aKhP8wHwYDVR0jBBgwFoAUkWHoKi2hDlS1/rYpcT/Ue+aKhP8w\n"
        "DQYJKoZIhvcNAQELBQADggEBADrrLcBY7gDXN8/0KHvPbGwMrEAJcnF9z4MBxRvt\n"
        "rEoRxhlvRZzPi7w/868xbipwwnksZsn0QNIiAZ6XzbwvIFG01ONJET+OzDy6ZqUb\n"
        "YmJI09EOe9/Hst8Fac2D14Oyw0+6KTqZW7WWrP2TAgv8/Uox2S05pCWNfJpRZxOv\n"
        "Lr4DZmnXBJCMNMY/X7xpcjylq+uCj118PBobfH9Oo+iAJ4YyjOLmX3bflKIn1Oat\n"
        "vdJBtXCj3phpfuf56VwKxoxEVR818GqPAHnz9oVvye4sQqBp/2ynrKFxZKUaJtk0\n"
        "7UeVbtecwnQTrlcpWM7ACQC0OO0M9+uNjpKIbksv1s11xu0=\n"
        "-----END CERTIFICATE-----\n";

    QSslConfiguration sslConfig = QSslConfiguration::defaultConfiguration();
    QList<QSslCertificate> caCertificates = sslConfig.caCertificates();
    caCertificates.append(QSslCertificate::fromData(aliyunIotRootCaPem, QSsl::Pem));
    sslConfig.setCaCertificates(caCertificates);
    sslConfig.setPeerVerifyMode(QSslSocket::VerifyPeer);
    sslConfig.setProtocol(QSsl::TlsV1_2OrLater);
    return sslConfig;
}

QString mqttClientStateName(QMqttClient::ClientState state)
{
    switch(state) {
    case QMqttClient::Disconnected:
        return QStringLiteral("Disconnected");
    case QMqttClient::Connecting:
        return QStringLiteral("Connecting");
    case QMqttClient::Connected:
        return QStringLiteral("Connected");
    }

    return QStringLiteral("Unknown");
}

QString mqttSubscriptionStateName(QMqttSubscription::SubscriptionState state)
{
    switch(state) {
    case QMqttSubscription::Unsubscribed:
        return QStringLiteral("Unsubscribed");
    case QMqttSubscription::SubscriptionPending:
        return QStringLiteral("SubscriptionPending");
    case QMqttSubscription::Subscribed:
        return QStringLiteral("Subscribed");
    case QMqttSubscription::UnsubscriptionPending:
        return QStringLiteral("UnsubscriptionPending");
    case QMqttSubscription::Error:
        return QStringLiteral("Error");
    }

    return QStringLiteral("Unknown");
}

} // namespace

AliyunMqttClient::AliyunMqttClient(QObject *parent)
    : QObject(parent),
      m_client(new QMqttClient(this)),
      m_reconnectTimer(new QTimer(this))
{
    qRegisterMetaType<AliyunSensorData>("AliyunSensorData");

    m_client->setProtocolVersion(QMqttClient::MQTT_3_1_1);
    m_client->setKeepAlive(60);

    m_reconnectTimer->setSingleShot(true);
    m_reconnectTimer->setInterval(m_reconnectIntervalMs);
    connect(m_reconnectTimer,&QTimer::timeout,this,&AliyunMqttClient::reconnectToAliyun);

    connect(m_client,&QMqttClient::connected,this,[this](){
        qInfo().noquote()
            << QStringLiteral("MQTT 已连接: host=%1，clientId=%2，开始订阅")
                   .arg(m_client->hostname(), plainClientId());
        m_reconnectTimer->stop();
        m_manualDisconnect = false;
        subscribeConfiguredTopics();
        emit connected();
    });

    connect(m_client,&QMqttClient::disconnected,this,[this](){
        qWarning().noquote()
            << QStringLiteral("MQTT 已断开: state=%1，error=%2")
                   .arg(mqttClientStateName(m_client->state()),
                        mqttErrorMessage(m_client->error()));
        emit disconnected();
        scheduleReconnect();
    });
    connect(m_client,&QMqttClient::stateChanged,this,[this](QMqttClient::ClientState state){
        qInfo().noquote()
            << QStringLiteral("MQTT state changed: %1").arg(mqttClientStateName(state));
        emit stateChanged(state);
    });
    connect(m_client,&QMqttClient::messageReceived,
            this,&AliyunMqttClient::handleMessageReceived);

    connect(m_client,&QMqttClient::errorChanged,this,[this](QMqttClient::ClientError error){
        if(error == QMqttClient::NoError)
            return;

        const QString message = mqttErrorMessage(error);
        qWarning().noquote()
            << QStringLiteral("MQTT 连接错误: %1，host=%2，port=%3，clientId=%4")
                   .arg(message,
                        m_client->hostname(),
                        QString::number(m_client->port()),
                        plainClientId());
        emit errorOccurred(QStringLiteral("连接失败: %1").arg(message));
    });
}

void AliyunMqttClient::configure(const Config &config)
{
    m_config = config;
}

void AliyunMqttClient::setSubscribeTopics(const QStringList &topics)
{
    m_config.subscribeTopics = topics;
}

QStringList AliyunMqttClient::subscribeTopics() const
{
    return m_config.subscribeTopics;
}

void AliyunMqttClient::setAutoReconnectEnabled(bool enabled, int intervalMs)
{
    m_autoReconnectEnabled = enabled;

    if(intervalMs > 0)
        m_reconnectIntervalMs = intervalMs;

    m_reconnectTimer->setInterval(m_reconnectIntervalMs);

    if(!m_autoReconnectEnabled)
        m_reconnectTimer->stop();
}

void AliyunMqttClient::connectToAliyun()
{
    if(!validateConfig())
        return;

    m_manualDisconnect = false;
    m_reconnectTimer->stop();

    if(m_client->state() != QMqttClient::Disconnected)
        m_client->disconnectFromHost();

    applyConnectionOptions();
    const QSslConfiguration sslConfig = aliyunSslConfiguration();
    qInfo().noquote()
        << QStringLiteral("MQTT 正在连接: host=%1，port=%2，clientId=%3")
               .arg(m_client->hostname(),
                    QString::number(m_client->port()),
                    plainClientId());
    m_client->connectToHostEncrypted(sslConfig);
}

void AliyunMqttClient::disconnectFromAliyun()
{
    m_manualDisconnect = true;
    m_reconnectTimer->stop();
    m_client->disconnectFromHost();
}

bool AliyunMqttClient::isConnected() const
{
    return m_client->state() == QMqttClient::Connected;
}

QMqttClient::ClientState AliyunMqttClient::state() const
{
    return m_client->state();
}

QString AliyunMqttClient::defaultPropertySetTopic() const
{
    if(m_config.productKey.isEmpty() || m_config.deviceName.isEmpty())
        return QString();

    return QStringLiteral("/sys/%1/%2/thing/service/property/set")
        .arg(m_config.productKey,m_config.deviceName);
}

static QString aliyunRuleEnginePropertySetTopic(const QString &productKey, const QString &deviceName)
{
    if(productKey.isEmpty() || deviceName.isEmpty())
        return QString();

    return QStringLiteral("/%1/%2/thing/service/property/set")
        .arg(productKey, deviceName);
}

bool AliyunMqttClient::validateConfig()
{
    if(m_config.productKey.isEmpty()
        || m_config.deviceName.isEmpty()
        || m_config.deviceSecret.isEmpty()
        || m_config.regionId.isEmpty()) {
        const QString message = QStringLiteral("阿里云 MQTT 配置不完整");
        qWarning().noquote() << QStringLiteral("MQTT 连接错误: %1").arg(message);
        emit errorOccurred(message);
        return false;
    }

    return true;
}

void AliyunMqttClient::applyConnectionOptions()
{
    const QString timestamp = QString::number(QDateTime::currentMSecsSinceEpoch());

    m_client->setHostname(brokerHost());
    m_client->setPort(m_config.port);
    m_client->setClientId(mqttClientId(timestamp));
    m_client->setUsername(username());
    m_client->setPassword(password(timestamp));
    m_client->setCleanSession(true);
}

void AliyunMqttClient::subscribeConfiguredTopics()
{
    QStringList topics;
    topics << defaultPropertySetTopic();
    topics << aliyunRuleEnginePropertySetTopic(m_config.productKey, m_config.deviceName);
    topics << m_config.subscribeTopics;

    topics.removeDuplicates();

    for(const QString &topic:topics) {
        if(topic.isEmpty())
            continue;

        QMqttSubscription *subscription = m_client->subscribe(QMqttTopicFilter(topic),1);
        if(subscription) {
            qInfo().noquote() << QStringLiteral("MQTT subscribe request: %1").arg(topic);
            connect(subscription, &QMqttSubscription::stateChanged,
                    this, [topic, subscription](QMqttSubscription::SubscriptionState state) {
                qInfo().noquote()
                    << QStringLiteral("MQTT subscription state: topic=%1，state=%2，qos=%3，reasonCode=%4，reason=%5")
                           .arg(topic,
                                mqttSubscriptionStateName(state),
                                QString::number(subscription->qos()),
                                QString::number(static_cast<int>(subscription->reasonCode())),
                                subscription->reason());
                if(state == QMqttSubscription::Error) {
                    qWarning().noquote()
                        << QStringLiteral("MQTT 订阅被拒绝: topic=%1，reasonCode=%2，reason=%3")
                               .arg(topic,
                                    QString::number(static_cast<int>(subscription->reasonCode())),
                                    subscription->reason());
                }
            });
        } else {
            emit errorOccurred(QStringLiteral("订阅失败: %1").arg(topic));
        }
    }
}

void AliyunMqttClient::scheduleReconnect()
{
    if(!m_autoReconnectEnabled || m_manualDisconnect)
        return;

    if(!validateConfig())
        return;

    if(m_client->state() != QMqttClient::Disconnected || m_reconnectTimer->isActive())
        return;

    const QString message = QStringLiteral("连接断开，%1 秒后自动重连")
                                .arg(m_reconnectIntervalMs / 1000);
    qWarning().noquote() << QStringLiteral("MQTT %1").arg(message);
    emit errorOccurred(message);
    m_reconnectTimer->start();
}

void AliyunMqttClient::reconnectToAliyun()
{
    if(m_client->state() != QMqttClient::Disconnected)
        return;

    connectToAliyun();
}

void AliyunMqttClient::handleMessageReceived(const QByteArray &message, const QMqttTopicName &topic)
{
    const QString topicName = topic.name();
    qInfo().noquote()
        << QStringLiteral("MQTT received: topic=%1, payload=%2")
               .arg(topicName, QString::fromUtf8(message));
    emit rawMessageReceived(topicName,message);

    QJsonParseError parseError;
    const QJsonDocument doc = QJsonDocument::fromJson(message,&parseError);

    if(parseError.error != QJsonParseError::NoError || !doc.isObject()) {
        emit errorOccurred(QStringLiteral("收到非 JSON 消息: %1").arg(topicName));
        return;
    }

    const QJsonObject root = doc.object();
    emit jsonMessageReceived(topicName,root);

    AliyunSensorData data;
    if(parseSensorData(root,&data))
        emit sensorDataReceived(data);
}

QString AliyunMqttClient::brokerHost() const
{
    const QString configuredHost = normalizedMqttHost(m_config.mqttHostUrl);
    if(!configuredHost.isEmpty())
        return configuredHost;

    return QStringLiteral("%1.iot-as-mqtt.%2.aliyuncs.com")
        .arg(m_config.productKey,m_config.regionId);
}

QString AliyunMqttClient::plainClientId() const
{
    if(!m_config.clientId.isEmpty())
        return m_config.clientId;

    return QStringLiteral("%1.%2").arg(m_config.productKey,m_config.deviceName);
}

QString AliyunMqttClient::mqttClientId(const QString &timestamp) const
{
    return plainClientId()
        + QStringLiteral("|securemode=2,signmethod=hmacsha256,timestamp=")
        + timestamp
        + QStringLiteral("|");
}

QString AliyunMqttClient::username() const
{
    return QStringLiteral("%1&%2").arg(m_config.deviceName,m_config.productKey);
}

QString AliyunMqttClient::password(const QString &timestamp) const
{
    const QString content =
        QStringLiteral("clientId") + plainClientId()
        + QStringLiteral("deviceName") + m_config.deviceName
        + QStringLiteral("productKey") + m_config.productKey
        + QStringLiteral("timestamp") + timestamp;

    const QByteArray sign = QMessageAuthenticationCode::hash(
        content.toUtf8(),
        m_config.deviceSecret.toUtf8(),
        QCryptographicHash::Sha256).toHex();

    return QString::fromUtf8(sign);
}

QString AliyunMqttClient::normalizedMqttHost(const QString &host)
{
    QString result = host.trimmed();

    if(result.startsWith(QStringLiteral("mqtt://")))
        result.remove(0, 7);
    else if(result.startsWith(QStringLiteral("mqtts://")))
        result.remove(0, 8);
    else if(result.startsWith(QStringLiteral("ssl://")))
        result.remove(0, 6);
    else if(result.startsWith(QStringLiteral("tcp://")))
        result.remove(0, 6);

    const int slashIndex = result.indexOf(QLatin1Char('/'));
    if(slashIndex >= 0)
        result = result.left(slashIndex);

    const int colonIndex = result.indexOf(QLatin1Char(':'));
    if(colonIndex >= 0)
        result = result.left(colonIndex);

    return result;
}

QString AliyunMqttClient::mqttErrorMessage(QMqttClient::ClientError error)
{
    switch(error) {
    case QMqttClient::NoError:
        return QStringLiteral("无错误");
    case QMqttClient::InvalidProtocolVersion:
        return QStringLiteral("协议版本不支持");
    case QMqttClient::IdRejected:
        return QStringLiteral("ClientId 被服务器拒绝");
    case QMqttClient::ServerUnavailable:
        return QStringLiteral("服务器不可用");
    case QMqttClient::BadUsernameOrPassword:
        return QStringLiteral("用户名或密码错误");
    case QMqttClient::NotAuthorized:
        return QStringLiteral("认证失败或无权限");
    case QMqttClient::TransportInvalid:
        return QStringLiteral("网络传输异常，请检查地址、端口、TLS 和网络连接");
    case QMqttClient::ProtocolViolation:
        return QStringLiteral("MQTT 协议违规");
    case QMqttClient::UnknownError:
        return QStringLiteral("未知错误");
    case QMqttClient::Mqtt5SpecificError:
        return QStringLiteral("MQTT 5 特定错误");
    }

    return QStringLiteral("未知错误码 %1").arg(static_cast<int>(error));
}

bool AliyunMqttClient::parseSensorData(const QJsonObject &root, AliyunSensorData *data)
{
    if(!data)
        return false;

    QJsonObject params;

    if(root.value(QStringLiteral("params")).isObject())
        params = root.value(QStringLiteral("params")).toObject();
    else if(root.value(QStringLiteral("Params")).isObject())
        params = root.value(QStringLiteral("Params")).toObject();
    else if(root.value(QStringLiteral("params")).isString()) {
        const QJsonDocument nestedDoc = QJsonDocument::fromJson(
            root.value(QStringLiteral("params")).toString().toUtf8());
        if(nestedDoc.isObject())
            params = nestedDoc.object();
    } else if(root.value(QStringLiteral("Params")).isString()) {
        const QJsonDocument nestedDoc = QJsonDocument::fromJson(
            root.value(QStringLiteral("Params")).toString().toUtf8());
        if(nestedDoc.isObject())
            params = nestedDoc.object();
    }
    else
        params = root;

    auto mergeAliyunItems = [](QJsonObject target, const QJsonObject &source) {
        const QJsonValue itemsValue = source.value(QStringLiteral("items"));
        if(!itemsValue.isObject())
            return target;

        const QJsonObject items = itemsValue.toObject();
        for(auto it = items.begin(); it != items.end(); ++it) {
            if(it.value().isObject()) {
                const QJsonObject itemObject = it.value().toObject();
                if(itemObject.contains(QStringLiteral("value")))
                    target.insert(it.key(), itemObject.value(QStringLiteral("value")));
                else
                    target.insert(it.key(), it.value());
            } else {
                target.insert(it.key(), it.value());
            }
        }

        return target;
    };

    params = mergeAliyunItems(params, params);
    params = mergeAliyunItems(params, root);

    if(params.isEmpty())
        return false;

    bool matched = false;
    data->rawParams = params;

    auto readNumber = [&params,&matched](const QStringList &keys,bool *hasValue,double *value) {
        for(const QString &key:keys) {
            const QJsonValue jsonValue = params.value(key);
            if(jsonValue.isDouble()) {
                *hasValue = true;
                *value = jsonValue.toDouble();
                matched = true;
                return;
            }

            if(jsonValue.isString()) {
                bool ok = false;
                const double numericValue = jsonValue.toString().toDouble(&ok);
                if(ok) {
                    *hasValue = true;
                    *value = numericValue;
                    matched = true;
                    return;
                }
            }
        }
    };

    auto readBool = [&params,&matched](const QStringList &keys,bool *hasValue,bool *value) {
        for(const QString &key:keys) {
            const QJsonValue jsonValue = params.value(key);
            if(jsonValue.isBool()) {
                *hasValue = true;
                *value = jsonValue.toBool();
                matched = true;
                return;
            }

            if(jsonValue.isDouble()) {
                *hasValue = true;
                *value = !qFuzzyIsNull(jsonValue.toDouble());
                matched = true;
                return;
            }

            if(jsonValue.isString()) {
                const QString text = jsonValue.toString().trimmed().toLower();
                if(text == QStringLiteral("true") || text == QStringLiteral("1")) {
                    *hasValue = true;
                    *value = true;
                    matched = true;
                    return;
                }

                if(text == QStringLiteral("false") || text == QStringLiteral("0")) {
                    *hasValue = true;
                    *value = false;
                    matched = true;
                    return;
                }
            }
        }
    };

    readNumber({QStringLiteral("temperature"),QStringLiteral("Temperature"),QStringLiteral("temp")},
               &data->hasTemperature,&data->temperature);
    readNumber({QStringLiteral("humidity"),QStringLiteral("humi"),QStringLiteral("Humidity")},
               &data->hasHumidity,&data->humidity);
    readNumber({QStringLiteral("smoke"),QStringLiteral("Smoke"),QStringLiteral("smokeConcentration"),QStringLiteral("smokeconcentration")},
               &data->hasSmoke,&data->smoke);
    readNumber({QStringLiteral("airPressure"),QStringLiteral("AirPressure"),QStringLiteral("airpressure"),QStringLiteral("pressure")},
               &data->hasAirPressure,&data->airPressure);

    readBool({QStringLiteral("fire"),QStringLiteral("Fire"),QStringLiteral("fireDetected"),QStringLiteral("flame")},
             &data->hasFire,&data->fireDetected);
    readBool({QStringLiteral("combustibleGas"),QStringLiteral("CombustibleGas"),QStringLiteral("combustible_gas"),QStringLiteral("gas")},
             &data->hasCombustibleGas,&data->combustibleGasDetected);

    return matched;
}

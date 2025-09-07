# IoT Data Platform Stack

## 📌 Overview
Stack นี้เป็นแพลตฟอร์ม IoT data platform ที่ใช้สำหรับการเก็บ, ประมวลผล และแสดงผลข้อมูลจาก IoT devices โดยใช้เครื่องมือดังนี้:

- **VerneMQ** → MQTT Broker สำหรับรับข้อความจาก IoT devices  
- **Kafka (KRaft mode)** → Event streaming platform สำหรับรองรับการประมวลผลข้อมูลแบบ real-time  
- **Kafka-UI** → Web UI สำหรับดู topics, messages, consumer groups  
- **InfluxDB 2.7** → Time-series database สำหรับเก็บ sensor data  
- **Prometheus** → Metrics collector  
- **Grafana** → Dashboard visualization  

> 📝 หมายเหตุ: VerneMQ ไม่มี `vmq_kafka_bridge` plugin ใน official image หากต้องการ forward MQTT → Kafka แนะนำให้เพิ่ม **Telegraf** หรือ **Kafka Connect MQTT Source Connector**

---

## ⚡ Services
| Service   | Port (Host) | Description |
|-----------|------------|-------------|
| VerneMQ   | `1883`     | MQTT Broker |
| VerneMQ API | `8888`   | HTTP API |
| Kafka     | `9092`     | Kafka external listener |
| Kafka     | `29092`    | Kafka internal listener (Docker network) |
| Kafka-UI  | `8080`     | Kafka management UI |
| InfluxDB  | `8086`     | Time-series DB |
| Prometheus| `9090`     | Metrics |
| Grafana   | `3000`     | Visualization dashboards |

---

## 🚀 Quick Start

### 1. Clone repository
```bash
git clone https://github.com/hanattaw/iot-class-2025-mini-project
cd iot-class-2025-mini-project

import time
import datetime
import paho.mqtt.client as mqtt#1.4.0
from pymodbus.client import ModbusSerialClient as ModbusClient
import json
import atexit
import signal
import sys

# 读取配置文件
def load_config(config_file="read_config.json"):
    with open(config_file, "r") as f:
        return json.load(f)

# 从配置文件加载参数
config = load_config()

# 配置参数
device_sn = config["device_sn"]  # 设备序列号
mqtt_broker = config["mqtt_broker"]
mqtt_port = config["mqtt_port"]
mqtt_username = config["mqtt_username"]
mqtt_password = config["mqtt_password"]
mqtt_topic = f"/sensor_data/SN_{device_sn}"
sensors_amount = config["sensors_amount"]  # 传感器数量
wait_time = config["wait_time"]  # 等待时间（秒），默认5分钟，可以根据需要调整

# Modbus RTU连接配置
modbus_port = config["modbus_port"]  # 串口端口
modbus_baudrate = config["modbus_baudrate"]  # 波特率
modbus_client = ModbusClient(port=modbus_port, baudrate=modbus_baudrate)

# 全局标志，表示MQTT是否连接
mqtt_connected = False

# 获取当前时间的函数
def get_current_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 连接Modbus RTU
def connect_modbus():
    print(f"{get_current_time()} - 正在连接到Modbus RTU设备...")
    if not modbus_client.connect():
        print(f"{get_current_time()} - 无法连接到Modbus RTU设备！")
        exit(1)
    else:
        print(f"{get_current_time()} - 成功连接到Modbus RTU设备。")

# 创建MQTT客户端并连接到MQTT服务器
def connect_mqtt():
    global mqtt_connected
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    print(f"{get_current_time()} - 正在连接到MQTT服务器 {mqtt_broker}:{mqtt_port}...")
    
    try:
        if client.connect(mqtt_broker, mqtt_port, 60) == 0:
            print(f"{get_current_time()} - 成功连接到MQTT服务器。")
            mqtt_connected = True  # 标记为已连接
        else:
            print(f"{get_current_time()} - MQTT连接失败，程序将退出。")
            exit(1)
    except Exception as e:
        print(f"{get_current_time()} - MQTT连接错误: {e}")
        exit(1)
    
    return client

    
# 读取传感器数据
def read_modbus_data(sensor_id):
    """
    读取指定传感器的Modbus数据
    """
    print(f"{get_current_time()} - 正在读取传感器 {sensor_id} 的数据...")
    result = modbus_client.read_holding_registers(0, 5, slave=sensor_id)
    
    if result.isError():
        print(f"{get_current_time()} - 传感器 {sensor_id} 读取Modbus数据失败！")
        return None
    
    registers = result.registers
    humidity = registers[0] / 10.0
    # 判断是否为负数并进行转换
    if registers[1] & 0x8000:
        temperature = -(0xFFFF - registers[1] + 1) / 10.0
    else:
        temperature = registers[1] / 10.0
    co2 = registers[2]
    light = registers[3] * 56635 + registers[4]
    
    print(f"{get_current_time()} - 传感器 {sensor_id} 读取成功: 湿度={humidity}, 温度={temperature}, CO2={co2}, 光照={light}")
    
    return {
        "humidity": humidity,
        "temperature": temperature,
        "co2": co2,
        "light": light
    }
    
# 发送数据到MQTT
def send_mqtt_data(client, data):
    """
    发送数据到MQTT服务器，包含重试机制
    """
    retry_count = 0
    while retry_count < 3:
        try:
            print(f"{get_current_time()} - 正在发送数据到MQTT服务器，主题: {mqtt_topic}...")
            client.publish(mqtt_topic, str(data))
            print(f"{get_current_time()} - 数据发送成功: {data}")
            return True
        except Exception as e:
            print(f"{get_current_time()} - MQTT发送失败，重试 {retry_count + 1} 次: {e}")
            retry_count += 1
            time.sleep(2)  # 重试间隔
    print(f"{get_current_time()} - MQTT发送失败，已重试3次")
    return False

# 清理操作
def cleanup():
    print(f"{get_current_time()} - 程序退出，正在进行清理工作...")
    try:
        modbus_client.close()
        print(f"{get_current_time()} - 已关闭Modbus连接。")
    except Exception as e:
        print(f"{get_current_time()} - 关闭Modbus连接时出错: {e}")

    try:
        # 仅在连接状态标志为True时才断开MQTT连接
        if mqtt_connected:
            mqtt_client.disconnect()
            print(f"{get_current_time()} - 已断开MQTT连接。")
    except Exception as e:
        print(f"{get_current_time()} - 关闭MQTT连接时出错: {e}")

# 捕获终止信号进行清理
def signal_handler(sig, frame):
    print(f"{get_current_time()} - 捕获到终止信号，正在清理...")
    atexit.unregister(cleanup)  # 确保退出时只执行一次清理
    cleanup()  # 调用清理函数
    sys.exit(0)  # 直接退出程序，防止atexit再执行

# 注册退出时的清理函数
atexit.register(cleanup)

# 注册信号处理器
def register_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)  # 捕获Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 捕获终止信号

def process_sensor_data():
    all_data = []
    print(f"{get_current_time()} - 正在处理传感器数据...")
    for sensor_id in range(1, sensors_amount + 1):
        data = read_modbus_data(sensor_id)
        if data:
            all_data.append(data)
    
    if not all_data:
        print(f"{get_current_time()} - 所有传感器采集失败。")
        return None  # 如果所有传感器都采集失败，返回None

    max_co2 = max(data["co2"] for data in all_data)
    max_humidity = max(data["humidity"] for data in all_data)
    max_temperature = max(data["temperature"] for data in all_data)
    max_light = max(data["light"] for data in all_data)

    print(f"{get_current_time()} - 计算最大值: CO2={max_co2}, 湿度={max_humidity}, 温度={max_temperature}, 光照={max_light}")
    
    read_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    result = {
        "co2": max_co2,
        "humidity": max_humidity,
        "light": max_light,
        "read_time": read_time,
        "temperature": max_temperature
    }
    
    return result

def main():
    connect_modbus()  # 连接到Modbus设备
    global mqtt_client
    mqtt_client = connect_mqtt()  # 连接到MQTT服务器
    register_signal_handlers()  # 注册信号处理器

    while True:
        print(f"\n{get_current_time()} - ----------------------")
        print(f"{get_current_time()} - 开始新的数据采集周期...")

        data = process_sensor_data()
        
        if data:
            send_mqtt_data(mqtt_client, data)
        else:
            print(f"{get_current_time()} - 所有传感器采集失败，跳过发送。")
        
        print(f"{get_current_time()} - 等待{wait_time}秒后继续采集...\n")
        time.sleep(wait_time)

if __name__ == "__main__": 
    main()

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import pymysql
import csv
import io
from datetime import datetime

app = Flask(__name__)
CORS(app)

# 配置 MySQL 数据库连接
DB_CONFIG = {
    "host": "localhost",       # MySQL 主机
    "user": "",            # 用户名
    "password": "",    # 密码
    "database": "",  # 数据库名
    "charset": "utf8mb4",      # 字符集
    "cursorclass": pymysql.cursors.DictCursor  # 返回字典形式的结果
}

# 建立 MySQL 数据库连接
def get_db_connection():
    conn = pymysql.connect(**DB_CONFIG)
    return conn
    
# 将 read_time 格式化为你需要的格式
def format_datetime(row):
    if 'read_time' in row:
        # 如果是 datetime 对象，则直接格式化为字符串
        if isinstance(row['read_time'], datetime):
            row['read_time'] = row['read_time'].strftime('%Y-%m-%d %H:%M:%S')  # 转换为目标格式
    return row
    
@app.route('/query_data', methods=['GET'])
def query_data():
    # 获取查询参数
    device_sn = request.args.get('device_sn')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # 参数验证
    if not device_sn or not start_date or not end_date:
        return jsonify({"error": "device_sn, start_date, and end_date are required"}), 400
    
    try:
        # 验证日期格式
        datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return jsonify({"error": "Incorrect date format, should be YYYY-MM-DD HH:MM:SS"}), 400

    # 验证设备编号
    if not device_sn.isdigit() or int(device_sn) not in range(1, 11):
        return jsonify({"error": "Invalid device_sn, must be a number between 1 and 10"}), 400

    # 构建 SQL 查询
    query = f'''
        SELECT read_time, temperature, co2, humidity, light
        FROM sensor_data_sn_{device_sn}
        WHERE read_time BETWEEN %s AND %s
        ORDER BY read_time
    '''

    try:
        # 执行查询
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        conn.close()

        # 格式化时间为 "YYYY-MM-DD HH:MM:SS"
        formatted_rows = [
            {**row, "read_time": row["read_time"].strftime("%Y-%m-%d %H:%M:%S")}
            for row in rows
        ]

        # 如果数据条数超过500条，返回CSV文件
        if len(rows) > 500:
            si = io.StringIO()
            csv_writer = csv.DictWriter(si, fieldnames=rows[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(formatted_rows)

            # 根据要求生成文件名
            filename = f"SN_{device_sn}_{start_date.replace(':', '-').replace(' ', '_')}_{end_date.replace(':', '-').replace(' ', '_')}.csv"

            return Response(
                si.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # 否则返回 JSON 数据
        return jsonify(formatted_rows)

    except pymysql.Error as e:
        return jsonify({"error": str(e)}), 500



# 查询所有设备或单个设备的最新数据
# 查询所有设备或单个设备的数据，支持返回指定数量
# 查询单个设备的最新数据
@app.route('/latest_data', methods=['GET'])
def latest_data():
    device_sn = request.args.get('device_sn')  # 获取设备编号参数
    data_id = request.args.get('id')  # 可选的 id 参数
    amount = request.args.get('amount', default=1)  # 可选的条数参数，默认返回 1 条


    # 验证设备编号（如果提供了）
    if device_sn and (not device_sn.isdigit() or int(device_sn) not in range(1, 11)):
        return jsonify({"error": "Invalid device_sn, must be a number between 1 and 10"}), 400

    # 验证 amount 是否为有效正整数
    if not str(amount).isdigit() or int(amount) <= 0:
        return jsonify({"error": "Invalid amount, must be a positive integer"}), 400
    amount = int(amount)  # 转换为整数

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        data = []

        # 情况 1: 无参数，查询所有设备的最新数据
        if not device_sn:
            for sn in range(1, 11):
                query = f'''
                    SELECT co2, humidity, light, read_time, temperature, id
                    FROM sensor_data_sn_{sn}
                    ORDER BY read_time DESC
                    LIMIT %s
                '''
                cursor.execute(query, (amount,))
                rows = cursor.fetchall()
                for row in rows:
                    row["device_sn"] = sn  # 添加设备编号信息
                    data.append(format_datetime(row))  # 格式化时间

        # 情况 2: 只有 device_sn，查询该设备的最新数据
        elif device_sn and not data_id:
            query = f'''
                SELECT co2, humidity, light, read_time, temperature, id
                FROM sensor_data_sn_{device_sn}
                ORDER BY read_time DESC
                LIMIT %s
            '''
            cursor.execute(query, (amount,))
            rows = cursor.fetchall()
            for row in rows:
                row["device_sn"] = device_sn  # 添加设备编号信息
                data.append(format_datetime(row))  # 格式化时间

        # 情况 3: 有 device_sn 和 id，查询指定设备的指定 id 的数据
        elif device_sn and data_id:
            query = f'''
                SELECT co2, humidity, light, read_time, temperature, id
                FROM sensor_data_sn_{device_sn}
                WHERE id = %s
            '''
            cursor.execute(query, (data_id,))
            row = cursor.fetchone()
            if row:
                row["device_sn"] = device_sn
                data.append(format_datetime(row))  # 格式化时间

        # 返回查询结果
        conn.close()

        # 设置返回的 JSON 数据编码为 UTF-8
        response = jsonify(data)
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response

    except pymysql.Error as e:
        return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    app.run(debug=False, threaded=True)  # 启动 Flask 服务器

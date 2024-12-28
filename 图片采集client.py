import cv2
import paramiko
import os
import time
import json
import signal
import sys


def load_config(config_file):
    """
    从配置文件中读取参数。
    """
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"无法加载配置文件: {e}")
        sys.exit(1)


def capture_image(device_sn, temp_dir):
    """
    使用OpenCV捕获图片并保存到临时目录。

    Args:
        device_sn (int): 设备编号，用于生成文件名。
        temp_dir (str): 临时目录路径。

    Returns:
        str: 保存的图片路径。
    """
    timestamp = time.strftime("%Y_%m_%d_%H_%M_%S")
    filename = f"{device_sn}_{timestamp}.bmp"
    local_image_path = os.path.join(temp_dir, filename)

    try:
        # 获取图片
        cap = cv2.VideoCapture(0)
        # 检查相机是否打开
        if not cap.isOpened():
            print("无法打开相机")
            exit()
		  
        # 设置相机分辨率（这里以1920x1080为例）
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 3840)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 2448)
        # 获取设置的分辨率
        width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        print(f"当前分辨率: {int(width)}x{int(height)}")

        # 捕获图像
        ret, frame = cap.read()
        if not ret:
            print("无法捕获图片，请检查摄像头是否正常工作。")
            cap.release()
            return None

        # 保存图片
        cv2.imwrite(local_image_path, frame)
        cap.release()
        print(f"图片已保存到本地：{local_image_path}")
        return local_image_path
    except Exception as e:
        print(f"图片采集失败：{e}")
        return None


def establish_sftp_connection(sftp_host, sftp_port, sftp_username, sftp_password):
    """
    建立并保持SFTP连接。

    Args:
        sftp_host (str): SFTP服务器主机地址。
        sftp_port (int): SFTP服务器端口号。
        sftp_username (str): SFTP服务器用户名。
        sftp_password (str): SFTP服务器密码。

    Returns:
        tuple: (ssh, sftp)连接对象。
    """
    try:
        # 连接到SFTP服务器
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)
        sftp = ssh.open_sftp()
        print("SFTP连接已成功建立。")
        return ssh, sftp
    except Exception as e:
        print(f"SFTP连接失败：{e}")
        sys.exit(1)


def upload_file(sftp, local_image_path, remote_folder_path):
    """
    上传文件到SFTP服务器。

    Args:
        sftp: SFTP连接对象。
        local_image_path (str): 本地图片路径。
        remote_folder_path (str): SFTP服务器远程路径。

    Returns:
        bool: 上传是否成功。
    """
    try:
        filename = os.path.basename(local_image_path)
        remote_file_path = os.path.join(remote_folder_path, filename)

        # 上传文件
        sftp.put(local_image_path, remote_file_path)
        print(f"文件已成功上传到SFTP服务器：{remote_file_path}")
        return True
    except Exception as e:
        print(f"上传失败：{e}")
        return False


def clean_temp_dir(temp_dir, max_files=5):
    """
    清理临时目录，确保文件数量不超过max_files。

    Args:
        temp_dir (str): 临时目录路径。
        max_files (int): 临时目录最多允许的文件数量。
    """
    try:
        files = sorted(os.listdir(temp_dir), key=lambda x: os.path.getctime(os.path.join(temp_dir, x)))
        while len(files) > max_files:
            file_to_remove = os.path.join(temp_dir, files[0])
            os.remove(file_to_remove)
            print(f"已清理临时文件：{file_to_remove}")
            files.pop(0)
    except Exception as e:
        print(f"清理临时目录失败：{e}")


def graceful_exit(signal_received, frame, ssh, sftp):
    """
    捕获退出信号，优雅地关闭程序。

    Args:
        signal_received: 捕获到的信号。
        frame: 当前堆栈帧。
        ssh: SSH连接对象。
        sftp: SFTP连接对象。
    """
    print("\n收到终止信号，程序即将退出...")
    if sftp:
        sftp.close()
        print("SFTP连接已安全关闭。")
    if ssh:
        ssh.close()
        print("SSH连接已安全关闭。")
    sys.exit(0)


def main(config_file):
    """
    主程序逻辑。

    Args:
        config_file (str): 配置文件路径。
    """
    config = load_config(config_file)

    # 从配置文件中读取参数
    device_sn = config['device_sn']
    temp_dir = config['temp_dir']
    sftp_host = config['sftp_host']
    sftp_port = config['sftp_port']
    sftp_username = config['sftp_username']
    sftp_password = config['sftp_password']
    remote_folder_path = config['remote_folder_path']
    capture_cycle = config['capture_cycle']  # 采集周期（秒）

    # 创建临时目录（如果不存在）
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # 建立SFTP连接
    ssh, sftp = establish_sftp_connection(sftp_host, sftp_port, sftp_username, sftp_password)

    # 注册信号处理器
    signal.signal(signal.SIGINT, lambda signal_received, frame: graceful_exit(signal_received, frame, ssh, sftp))
    signal.signal(signal.SIGTERM, lambda signal_received, frame: graceful_exit(signal_received, frame, ssh, sftp))

    while True:
        start_time = time.time()
        print("开始新一轮图片采集和上传任务...")

        # 捕获图片
        local_image_path = capture_image(device_sn, temp_dir)
        if not local_image_path:
            print("图片采集失败，跳过本轮任务。")
            continue

        # 上传图片
        success = upload_file(sftp, local_image_path, remote_folder_path)
        if not success:
            print("图片上传失败，本轮任务结束。")
            continue

        # 清理临时目录
        clean_temp_dir(temp_dir)

        # 等待下一个采集周期
        elapsed_time = time.time() - start_time
        sleep_time = max(0, capture_cycle - elapsed_time)
        if sleep_time > 0:
            print(f"等待{sleep_time:.2f}秒开始下一轮...")
            time.sleep(sleep_time)
        else:
            print("采集周期内未完成任务，立即开始下一轮任务。")


if __name__ == "__main__":
    # 配置文件路径
    config_file_path = "cap_config.json"

    # 主程序
    main(config_file_path)

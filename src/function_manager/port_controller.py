import gevent.lock
import logging
import socket

# a really simple port controller allocating port in a range with gevent concurrency safety
class PortController:
    def __init__(self, min_port, max_port):
        self.lock = gevent.lock.BoundedSemaphore()
        self.min_port = min_port
        self.max_port = max_port
        self.allocated_ports = set()  # 跟踪已分配的端口
        
        # 初始化时检查可用端口，排除已被占用的端口
        self.port_resource = self._get_available_ports(min_port, max_port)
        
    def _is_port_available(self, port):
        """检查端口是否可用"""
        try:
            # 尝试绑定到 0.0.0.0，以模拟 Docker 的行为
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0', port))
            sock.close()
            return True
        except OSError:
            # 端口被占用或其他错误
            return False
    
    def _get_available_ports(self, min_port, max_port):
        """获取指定范围内所有可用的端口"""
        available_ports = []
        occupied_count = 0
        
        for port in range(min_port, max_port):
            if self._is_port_available(port):
                available_ports.append(port)
            else:
                occupied_count += 1
        
        logging.info(f"[PORT_CONTROLLER] Initialized with {len(available_ports)} available ports "
                    f"(range: {min_port}-{max_port-1}, {occupied_count} ports occupied)")
        
        if len(available_ports) == 0:
            logging.error(f"[PORT_CONTROLLER] No available ports in range {min_port}-{max_port-1}")
            raise Exception(f"No available ports in range {min_port}-{max_port-1}")
        
        return available_ports
    
    def refresh_available_ports(self):
        """重新扫描并更新可用端口池"""
        try:
            self.lock.acquire()
            
            # 保存当前已分配的端口
            current_allocated = self.allocated_ports.copy()
            
            # 重新扫描可用端口
            new_available_ports = []
            for port in range(self.min_port, self.max_port):
                if port not in current_allocated and self._is_port_available(port):
                    new_available_ports.append(port)
            
            old_count = len(self.port_resource)
            self.port_resource = new_available_ports
            new_count = len(self.port_resource)
            
            logging.info(f"[PORT_CONTROLLER] Refreshed port pool: {old_count} -> {new_count} available ports")
            return True
            
        except Exception as e:
            logging.error(f"[PORT_CONTROLLER] Error refreshing available ports: {e}")
            return False
        finally:
            self.lock.release()
        
        
    def get(self):
        """获取一个可用端口"""
        try:
            self.lock.acquire()
            
            if len(self.port_resource) == 0:
                logging.error(f"[PORT_CONTROLLER] No idle port available. Range: {self.min_port}-{self.max_port}")
                raise Exception("no idle port")
            
            port = self.port_resource.pop()
            self.allocated_ports.add(port)
            return port
            
        except Exception as e:
            logging.error(f"[PORT_CONTROLLER] Error getting port: {e}")
            raise
        finally:
            self.lock.release()

    def put(self, port):
        """归还一个端口"""
        try:
            self.lock.acquire()
            
            # 检查端口是否在有效范围内
            if port < self.min_port or port >= self.max_port:
                logging.warning(f"[PORT_CONTROLLER] Invalid port {port} not in range [{self.min_port}, {self.max_port})")
                return False
            
            # 检查端口是否已经在资源池中
            if port in self.port_resource:
                logging.warning(f"[PORT_CONTROLLER] Port {port} already in resource pool")
                return False
            
            # 检查端口是否是我们分配的
            if port not in self.allocated_ports:
                logging.warning(f"[PORT_CONTROLLER] Port {port} was not allocated by this controller")
                return False
            
            self.port_resource.append(port)
            self.allocated_ports.remove(port)
            return True
            
        except Exception as e:
            logging.error(f"[PORT_CONTROLLER] Error returning port {port}: {e}")
            return False
        finally:
            self.lock.release()

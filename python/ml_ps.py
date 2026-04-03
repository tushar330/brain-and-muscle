import os
import sys
import grpc
from concurrent import futures
import numpy as np
import importlib.util

# Generated gRPC files (using the corrected topology prefix)
import topology_pb2
import topology_pb2_grpc

def load_user_plugin(file_path):
    """
    Dynamically loads the user's Python model file from the filesystem.
    This is what makes the platform generic!
    """
    if not file_path or not os.path.exists(file_path):
        print(f"[PS] FATAL: Missing user script at {file_path}")
        sys.exit(1)
        
    print(f"[PS] Loading user model from {file_path}...")
    spec = importlib.util.spec_from_file_location("user_model", file_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)
    return user_module

class ParameterServerServicer(topology_pb2_grpc.ParameterServerServicer):
    def __init__(self, user_script_path, max_staleness=3):
        # 1. Load the User's Model Architecture
        self.user_logic = load_user_plugin(user_script_path)
        
        # 2. Initialize the Global Weights via the User's hook
        print("[PS] Initializing global weights via user plugin...")
        self.global_weights = self.user_logic.initialize_weights()
        
        # 3. SSP (Stale Synchronous Parallel) State Tracking
        self.worker_epochs = {}       # Maps worker_id -> current_epoch
        self.max_staleness = max_staleness
        self.current_global_epoch = 0

    def PushGradients(self, request_iterator, context):
        """
        Receives streamed TensorChunks from ML Workers, reassembles them, 
        checks SSP staleness, and applies the user's optimizer.
        """
        worker_id = None
        current_epoch = None
        raw_bytes = bytearray()
        
        # 1. Reassemble the streamed chunks from the HTTP/2 network
        for chunk in request_iterator:
            worker_id = chunk.worker_id
            current_epoch = chunk.current_epoch
            raw_bytes.extend(chunk.raw_tensor_data)
            
        if not worker_id:
            return topology_pb2.PushAck(success=False, acknowledged_epoch=0)

        # 2. SSP Staleness Enforcement
        self.worker_epochs[worker_id] = current_epoch
        min_epoch = min(self.worker_epochs.values()) if self.worker_epochs else current_epoch
        
        if current_epoch - min_epoch > self.max_staleness:
            # If a worker is running too fast while others struggle, we reject its update
            # to prevent the model from skewing heavily toward one data partition.
            print(f"[PS] Worker {worker_id} is too fast (Epoch {current_epoch} vs Global Min {min_epoch}). Rejecting.")
            return topology_pb2.PushAck(success=False, acknowledged_epoch=current_epoch)

        # 3. Convert raw network bytes back to a Numpy array
        worker_gradients = np.frombuffer(raw_bytes, dtype=np.float32)

        # 4. Invoke the User's Optimizer!
        print(f"[PS] Applying gradients from {worker_id} for Epoch {current_epoch}...")
        self.global_weights = self.user_logic.apply_update(
            self.global_weights, 
            worker_gradients
        )

        # Advance the global clock if the slowest worker has caught up
        if min_epoch > self.current_global_epoch:
            self.current_global_epoch = min_epoch
            print(f"[PS] 🌐 Global Epoch advanced to {self.current_global_epoch}")

        return topology_pb2.PushAck(success=True, acknowledged_epoch=current_epoch)

def serve():
    # Read the environment variable injected by the Go Muscle
    user_script = os.environ.get("USER_SCRIPT_PATH")
    if not user_script:
        print("[PS] FATAL: USER_SCRIPT_PATH environment variable not set. Did the Go Muscle inject it?")
        sys.exit(1)

    # Initialize the high-throughput gRPC Thread Pool
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Initialize our Servicer with a default staleness of 3 epochs
    servicer = ParameterServerServicer(user_script, max_staleness=3)
    topology_pb2_grpc.add_ParameterServerServicer_to_server(servicer, server)
    
    # Bind to the Docker container's IP
    port = "50051"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[PS] Parameter Server running on port {port}. Awaiting ML Workers...")
    
    # Keep the server running infinitely
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
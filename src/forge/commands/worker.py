from forge.worker import ForgeWorker

def handle_worker():
    worker = ForgeWorker()
    print("Forge Worker is running...")
    worker.run()

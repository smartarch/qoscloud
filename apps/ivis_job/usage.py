from cloud_controller.middleware.user_agents import ComponentAgent


def update_dependency():
    agent.set_finished()


def recognize():
    pass


def finalize():
    agent.set_finished()


def initialize():
    collection = agent.get_mongo_agent()


agent = ComponentAgent(
    finalize_call=finalize,
    initialize_call=initialize,
    update_call=update_dependency
)

agent.register_probe("recognize", recognize)
agent.start()
agent.set_ready()


agent.start_probe_measurement("recognize")

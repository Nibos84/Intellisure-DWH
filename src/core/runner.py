        if agent_type == "generic_rest_api":
            agent = SmartIngestionAgent(pipeline_name, self.manifest_config)
            agent.run()
        elif agent_type == "generic_ai_transformer":
            agent = GenericAITransformer(pipeline_name, self.manifest_config)
            agent.run()
        else:
            logger.error(f"Unknown agent type: {agent_type}")

def run_pipeline(manifest_path: str):
    runner = PipelineRunner(manifest_path)
    runner.run()

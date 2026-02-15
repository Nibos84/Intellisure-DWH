import sys
import os

# Ensure src is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.agents.mas.orchestrator import orchestrator
from src.core.runner import run_pipeline
from src.core.config import config

def main():
    print("========================================")
    print("Agentic Data Engineer - Agile AI Team")
    print(f"Environment: {config.env}")
    print("MAS Roles: Researcher, Architect, Engineer")
    print("========================================")
    
    while True:
        try:
            print("\nWhat would you like to achieve? (e.g. 'Ingest KNMI data')")
            user_input = input("> ").strip()
            
            if user_input.lower() in ["exit", "quit"]:
                break
                
            if not user_input:
                continue

            # --- Phase 1: Research & Plan ---
            print("\n[Orchestrator] Engaging Agile AI Team...")
            context = orchestrator.start_mission(user_input)
            
            print("\n------------------------------------")
            print("🕵️  RESEARCHER FINDINGS:")
            print(context['research'])
            print("\n🏗️  ARCHITECT PROPOSAL:")
            print(context['plan'])
            print("------------------------------------")
            
            proceed = input("\nDo you approve this plan? (y/n): ")
            if proceed.lower() != 'y':
                print("[Orchestrator] Plan rejected. Team stands down.")
                continue

            # --- Phase 2: Build & Execute ---
            print("\n[Orchestrator] Engaging Engineer to write Manifest...")
            manifest_content = orchestrator.execute_mission(context)
            
            # Save Manifest
            # Sanitize filename
            safe_name = "".join([c if c.isalnum() else "_" for c in context['mission'].lower()])[:30]
            manifest_filename = f"manifests/mas_{safe_name}.yaml"
            
            with open(manifest_filename, "w") as f:
                f.write(manifest_content)
                
            print(f"\n[Engineer] Manifest generated: {manifest_filename}")
            print("--- Manifest Preview ---")
            print(manifest_content)
            print("------------------------")
            
            confirm = input("Execute this pipeline? (y/n): ")
            if confirm.lower() == 'y':
                run_pipeline(manifest_filename)
                print("\n[Orchestrator] Mission Accomplished.")
            else:
                print("\n[Orchestrator] Execution cancelled.")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"\n[Error] {e}")

if __name__ == "__main__":
    main()

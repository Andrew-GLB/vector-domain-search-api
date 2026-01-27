import subprocess

from app.services.ai_service import AIService


class GitAIHelper:
    """Utility to automate Git tasks using Gemini AI.
    Fulfills the PDF requirement: 'Generate descriptive git commit messages'.
    """

    def __init__(self) -> None:
        self.ai = AIService()

    def get_code_diff(self) -> str:
        """Fetches the difference between local code and origin/main."""
        try:
            # We compare current changes to origin/main
            result = subprocess.run(
                ["git", "diff", "origin/main"],
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8"
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            return f"Error fetching diff: {e}"

    def generate_commit_summary(self) -> str:
        """Sends the code diff to Gemini via AIService and returns a commit message."""
        diff = self.get_code_diff()

        if not diff or not diff.strip():
            return "No changes detected between local and origin/main."

        prompt = (
            "Analyze the following git diff and write a professional, descriptive "
            "git commit message using 'Conventional Commits' format (feat:, fix:, refactor:). "
            "Limit the first line to 50 characters. "
            f"\n\nDIFF:\n{diff}"
        )

        # Call the generic completion method in our refactored AIService
        return self.ai.get_completion(prompt)

if __name__ == "__main__":
    helper = GitAIHelper()
    print("\n--- AI Generated Commit Message ---\n")
    print(helper.generate_commit_summary())
    print("\n-----------------------------------\n")

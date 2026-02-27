You are selecting resume modules for a role.

{shared_restrictions}

Job context:
{row_context}

Resume module inventory:
{resume_context}

Task:
1. Choose the best-fit experience IDs and project IDs for this role.
2. Select at most 3 experiences and at most 4 projects.
3. Explain selection briefly.

Return STRICT JSON only (no markdown, no extra text):
{{
  "selected_experience_ids": ["<id1>", "<id2>"],
  "selected_project_ids": ["<id1>", "<id2>", "<id3>"],
  "why": ["<short reason>", "<short reason>"]
}}

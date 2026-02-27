You are rewriting selected resume modules for target job fit.

{shared_restrictions}

Job context:
{row_context}

Selected modules:
{selected_context}

Task:
1. Rewrite only selected experiences/projects.
2. Never invent metrics, tools, roles, or claims not present in selected modules.
3. Keep experience entries to exactly:
   - 3 impact bullets
   - 1 dedicated technical bullet in field `tech_bullet`
4. Keep project entries to 2-3 bullets and include `code_link`.
5. Keep wording concise and role-matched.
6. Word targets:
   - Experience bullet max words: {experience_bullet_max_words}
   - Project bullet max words: {project_bullet_max_words}
7. Every bullet must be a complete, meaningful sentence fragment with a clear action + outcome.
8. Avoid telegraphic fragments, dangling clauses, and vague phrases like "improved things" or "handled tasks".
9. Keep each bullet tight but readable, ideally 8-16 words.

Return STRICT JSON only (no markdown, no comments, no extra text):
{{
  "experiences": [
    {{
      "id": "<experience_id>",
      "bullets": ["<b1>", "<b2>", "<b3>"],
      "tech_bullet": "Tech: <comma-separated stack>"
    }}
  ],
  "projects": [
    {{
      "id": "<project_id>",
      "bullets": ["<b1>", "<b2>", "<optional b3>"],
      "code_link": "https://..."
    }}
  ]
}}

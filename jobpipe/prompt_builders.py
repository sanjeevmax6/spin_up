from __future__ import annotations

from jobpipe.models import JobRow, RuleSet


def _shared_context(row: JobRow, rules: RuleSet, baseline_profile: str) -> str:
    return f"""
You are preparing high-quality job application materials.
Goal: maximize interview rate while obeying all hard constraints.

Candidate baseline profile:
{baseline_profile}

Job details:
- Company: {row.company}
- Role: {row.role_title}
- Location: {row.location}
- Work mode: {row.work_mode or "N/A"}
- Seniority: {row.seniority or "N/A"}
- Must-haves: {row.must_haves or "N/A"}
- Nice-to-haves: {row.nice_to_haves or "N/A"}
- Notes: {row.notes or "N/A"}
- Job description:
{(row.job_description_text or "").strip()}

Hard constraints (must obey):
- Must include: {", ".join(rules.must_include_constraints) or "None"}
- Banned claims: {", ".join(rules.banned_claims) or "None"}
- Style constraints: {", ".join(rules.style_constraints) or "None"}

Do not invent unverifiable facts.
""".strip()


def build_resume_prompt(row: JobRow, rules: RuleSet, baseline_profile: str) -> str:
    return (
        _shared_context(row, rules, baseline_profile)
        + """

Output:
- Tailored bullet bank for resume updates
- 8-12 bullets
- Group by themes: impact, technical fit, domain fit
- Include high-value keywords from JD naturally
- Markdown format
""".rstrip()
    )


def build_cover_letter_prompt(row: JobRow, rules: RuleSet, baseline_profile: str) -> str:
    return (
        _shared_context(row, rules, baseline_profile)
        + """

Output:
- Short narrative cover letter, 250-350 words
- Focus on role fit, measurable impact, and motivation
- Markdown format
""".rstrip()
    )


def build_linkedin_prompt(row: JobRow, rules: RuleSet, baseline_profile: str) -> str:
    return (
        _shared_context(row, rules, baseline_profile)
        + """

Output:
- LinkedIn target personas for recruiter, hiring manager, potential teammate
- For each persona provide:
  - Why this persona matters
  - Search strategy (keywords and filters)
  - Connection request draft
  - Follow-up draft
- Markdown format
""".rstrip()
    )

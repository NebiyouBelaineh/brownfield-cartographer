## Target Codebase

**Repository:** apache/airflow

**URL:** https://github.com/apache/airflow

**Local path:** `/home/neba/tenx/week4/airflow`

**Composition:** Python (core scheduler, operators, DAGs), TypeScript (UI), YAML (configs), SQL (example queries)

**Why chosen:** Personal interest in workflow scheduling, monitoring, and orchestration systems.

**Size:** 8,000+ files across multiple languages — a genuine brownfield codebase.

---

# Reconnaissance

## The Five FDE Day-One Questions

1. What is the primary data ingestion path?

- Identifying the entry point for data was difficult due to the complexity of the codebase and the lack of concise documentation. Without a clear overview or map, it was challenging to trace which components handle data ingestion or where data enters the system.

2. What are the 3-5 most critical output datasets/endpoints?

- Determining the most important outputs or endpoints required a level of familiarity not easily gained on a first pass through the code. The scattered nature of relevant files and absence of summary documentation made it hard to tell which outputs are paramount.

3. What is the blast radius if the most critical module fails?

- Understanding the potential impact of a module's failure depends on knowing the dependencies and critical pathways in the system. Without that context or visual aids, it was a challenge to map out the consequences or interconnectedness within the codebase.

4. Where is the business logic concentrated vs. distributed?

- Pinpointing the locations of business logic was hampered by the size and modular nature of the project. It was difficult to tell at a glance whether responsibilities were centralized or scattered, as files related to decision-making seemed to be spread throughout multiple areas.

5. What has changed most frequently in the last 90 days (git velocity map)?

- To answer this question requires insight into commit history and recent code activity. With only surface-level access, there was insufficient opportunity to review change patterns or track fast-evolving components.

## Difficulty

- 30 minutes is a very short period to get oriented and synthesize meaningful answers to high-level architecture questions, especially for a large and unfamiliar codebase.
- The lack of time hindered any deep exploration, making it difficult to understand key changes, technical choices, and the rationale behind architectural decisions without consulting issues, pull requests, and releases.

### What was the hardest?

- The most challenging aspect was dealing with the sheer volume and structure of files and folders. Properly reviewing them and any supporting documentation that might explain the system's workings wasn’t feasible in such a short window.

### Where I got lost?

- I couldn’t get beyond the README.md in the root directory. I spent most of my time trying to get an initial understanding of the product, rather than being able to dive deeper into the technical specifics.

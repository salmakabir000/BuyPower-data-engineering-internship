from html import escape
from pathlib import Path


def generate_docs(project_name, models, graph, output_path="target/index.html"):
    model_sections = []

    for model in models:
        name = model["name"]
        materialized = model.get("materialized", "table")
        dependencies = sorted(graph[name])
        columns = model.get("columns", {})

        column_rows = ""

        for column, description in columns.items():
            column_rows += f"""
                <tr>
                    <td>{escape(column)}</td>
                    <td>{escape(description)}</td>
                </tr>
            """

        if not column_rows:
            column_rows = """
                <tr>
                    <td colspan="2">No column descriptions available.</td>
                </tr>
            """

        dependency_list = (
            ", ".join(escape(dep) for dep in dependencies)
            if dependencies
            else "No model dependencies"
        )

        sql_code = escape(model["sql"])

        section = f"""
            <section class="model">
                <h2>{escape(name)}</h2>

                <p>
                    <strong>Materialization:</strong>
                    {escape(materialized)}
                </p>

                <p>
                    <strong>Dependencies:</strong>
                    {dependency_list}
                </p>

                <h3>Column descriptions</h3>

                <table>
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                        {column_rows}
                    </tbody>
                </table>

                <h3>SQL</h3>
                <pre><code>{sql_code}</code></pre>
            </section>
        """

        model_sections.append(section)

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{escape(project_name)} Documentation</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                max-width: 1000px;
                margin: 40px auto;
                padding: 0 20px;
                color: #263238;
                background: #f5f7fa;
            }}

            h1, h2, h3 {{
                color: #17324d;
            }}

            .model {{
                background: white;
                padding: 24px;
                margin: 24px 0;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}

            th, td {{
                text-align: left;
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}

            pre {{
                background: #17212b;
                color: #f1f5f9;
                padding: 16px;
                border-radius: 6px;
                overflow-x: auto;
            }}

            code {{
                font-family: Consolas, monospace;
            }}
        </style>
    </head>
    <body>
        <h1>{escape(project_name)}</h1>
        <p>Mini dbt project documentation</p>
        <p>Total models: {len(models)}</p>

        {''.join(model_sections)}
    </body>
    </html>
    """

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html_content, encoding="utf-8")

    print(f"Documentation generated: {output_file}")

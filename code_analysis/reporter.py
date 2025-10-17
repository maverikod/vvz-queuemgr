"""
Reporter for the code mapper.

This module contains reporting functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import yaml
from typing import Dict, List, Any
from pathlib import Path


class CodeReporter:
    """Code reporting functionality."""

    def __init__(self, output_dir: Path):
        """Initialize code reporter."""
        self.output_dir = output_dir

    def generate_code_map(self, code_map: Dict[str, Any]) -> None:
        """Generate code map YAML file."""
        yaml_file = self.output_dir / "code_map.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(code_map, f, default_flow_style=False, allow_unicode=True)
        print(f"INFO:__main__:YAML карта кода сохранена в файл: {yaml_file}")

    def generate_issues_report(self, issues: Dict[str, List[Any]]) -> None:
        """Generate issues report YAML file."""
        yaml_file = self.output_dir / "code_issues.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump({"issues": issues}, f, default_flow_style=False, allow_unicode=True)
        print(f"INFO:__main__:YAML отчет о проблемах сохранен в файл: {yaml_file}")

    def generate_method_index(self, code_map: Dict[str, Any]) -> None:
        """Generate method index YAML file."""
        method_index = {}
        
        # Index methods by class
        for key, class_info in code_map.get("classes", {}).items():
            class_name = class_info["name"]
            if class_name not in method_index:
                method_index[class_name] = []
            
            for method in class_info.get("methods", []):
                method_index[class_name].append(method)
        
        yaml_file = self.output_dir / "method_index.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(method_index, f, default_flow_style=False, allow_unicode=True)
        print(f"INFO:__main__:YAML индекс методов сохранен в файл: {yaml_file}")

    def print_summary(self, issues: Dict[str, List[Any]], max_lines: int) -> None:
        """Print analysis summary."""
        total_issues = sum(len(issue_list) for issue_list in issues.values())
        
        print(f"Анализ завершен!")
        print(f"Созданы YAML отчеты в каталоге: {self.output_dir}")
        print(f"- code_map.yaml - карта кода")
        print(f"- code_issues.yaml - проблемы в коде")
        print(f"- method_index.yaml - индекс методов")
        print(f"Лимит строк на файл: {max_lines}")
        print(f"Всего найдено проблем: {total_issues}")
        
        # Files too large
        files_too_large = issues.get("files_too_large", [])
        if files_too_large:
            print(f"Файлов, превышающих лимит: {len(files_too_large)}")
            for file_info in files_too_large:
                print(f"  - {file_info['file']}: {file_info['lines']} строк (превышение на {file_info['exceeds_limit']})")
        
        # Summary
        summary = {
            "any_type_usage": len(issues.get("any_type_usage", [])),
            "classes_without_docstrings": len(issues.get("classes_without_docstrings", [])),
            "files_too_large": len(issues.get("files_too_large", [])),
            "files_without_docstrings": len(issues.get("files_without_docstrings", [])),
            "generic_exception_usage": len(issues.get("generic_exception_usage", [])),
            "imports_in_middle": len(issues.get("imports_in_middle", [])),
            "max_lines_limit": max_lines,
            "methods_with_pass": len(issues.get("methods_with_pass", [])),
            "methods_without_docstrings": len(issues.get("methods_without_docstrings", [])),
            "not_implemented_in_non_abstract": len(issues.get("not_implemented_in_non_abstract", [])),
            "total_issues": total_issues
        }
        
        # Save summary to issues file
        issues_with_summary = {"issues": issues, "summary": summary}
        yaml_file = self.output_dir / "code_issues.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(issues_with_summary, f, default_flow_style=False, allow_unicode=True)

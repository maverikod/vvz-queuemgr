"""
Code analyzer for the code mapper.

This module contains the core analysis functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import os
import ast
import json
from typing import Dict, List, Any, Set
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class CodeAnalyzer:
    """Core code analyzer functionality."""

    def __init__(self, root_dir: str = ".", output_dir: str = "code_analysis", max_lines: int = 400):
        """Initialize code analyzer."""
        self.root_dir = Path(root_dir)
        self.output_dir = Path(output_dir)
        self.max_lines = max_lines
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(exist_ok=True)
        
        self.code_map = {
            "files": {},
            "classes": {},
            "functions": {},
            "imports": {},
            "dependencies": {},
        }
        # Issues tracking
        self.issues = {
            "methods_with_pass": [],
            "not_implemented_in_non_abstract": [],
            "methods_without_docstrings": [],
            "files_without_docstrings": [],
            "classes_without_docstrings": [],
            "files_too_large": [],
            "any_type_usage": [],
            "generic_exception_usage": [],
            "imports_in_middle": [],
        }

    def analyze_file(self, file_path: Path) -> None:
        """Analyze a single Python file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # Check file size
            lines = content.split('\n')
            if len(lines) > self.max_lines:
                self.issues["files_too_large"].append({
                    "file": str(file_path),
                    "lines": len(lines),
                    "limit": self.max_lines,
                    "exceeds_limit": len(lines) - self.max_lines
                })
            
            # Check for file docstring
            if not self._has_file_docstring(tree):
                self.issues["files_without_docstrings"].append(str(file_path))
            
            # Analyze AST
            self._analyze_ast(tree, file_path)
            
        except (OSError, IOError, ValueError, SyntaxError, UnicodeDecodeError) as e:
            logger.error(f"Error analyzing file {file_path}: {e}")

    def _has_file_docstring(self, tree: ast.AST) -> bool:
        """Check if file has a docstring."""
        if not tree.body:
            return False
        
        first_node = tree.body[0]
        return (isinstance(first_node, ast.Expr) and 
                isinstance(first_node.value, ast.Constant) and 
                isinstance(first_node.value.value, str))

    def _analyze_ast(self, tree: ast.AST, file_path: Path) -> None:
        """Analyze AST nodes."""
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                self._analyze_class(node, file_path)
            elif isinstance(node, ast.FunctionDef):
                self._analyze_function(node, file_path)
            elif isinstance(node, ast.Import):
                self._analyze_import(node, file_path)
            elif isinstance(node, ast.ImportFrom):
                self._analyze_import_from(node, file_path)

    def _analyze_class(self, node: ast.ClassDef, file_path: Path) -> None:
        """Analyze class definition."""
        class_name = node.name
        class_info = {
            "name": class_name,
            "file": str(file_path),
            "line": node.lineno,
            "bases": [base.id if isinstance(base, ast.Name) else str(base) for base in node.bases],
            "methods": [],
            "docstring": ast.get_docstring(node),
        }
        
        # Check for class docstring
        if not class_info["docstring"]:
            self.issues["classes_without_docstrings"].append({
                "class": class_name,
                "file": str(file_path),
                "line": node.lineno
            })
        
        # Analyze methods
        for method in node.body:
            if isinstance(method, ast.FunctionDef):
                self._analyze_method(method, file_path, class_name)
                class_info["methods"].append(method.name)
        
        self.code_map["classes"][f"{file_path}:{class_name}"] = class_info

    def _analyze_function(self, node: ast.FunctionDef, file_path: Path) -> None:
        """Analyze function definition."""
        func_info = {
            "name": node.name,
            "file": str(file_path),
            "line": node.lineno,
            "args": [arg.arg for arg in node.args.args],
            "docstring": ast.get_docstring(node),
        }
        
        # Check for function docstring
        if not func_info["docstring"]:
            self.issues["methods_without_docstrings"].append({
                "class": None,
                "file": str(file_path),
                "line": node.lineno,
                "method": node.name
            })
        
        self.code_map["functions"][f"{file_path}:{node.name}"] = func_info

    def _analyze_method(self, node: ast.FunctionDef, file_path: Path, class_name: str) -> None:
        """Analyze method definition."""
        # Check for method docstring
        if not ast.get_docstring(node):
            self.issues["methods_without_docstrings"].append({
                "class": class_name,
                "file": str(file_path),
                "line": node.lineno,
                "method": node.name
            })
        
        # Check for pass statements
        if self._has_pass_statement(node):
            self.issues["methods_with_pass"].append({
                "class": class_name,
                "file": str(file_path),
                "line": node.lineno,
                "method": node.name
            })
        
        # Check for NotImplementedError in non-abstract methods
        if self._has_not_implemented_error(node) and not self._is_abstract_method(node):
            self.issues["not_implemented_in_non_abstract"].append({
                "class": class_name,
                "file": str(file_path),
                "line": node.lineno,
                "method": node.name
            })

    def _has_pass_statement(self, node: ast.FunctionDef) -> bool:
        """Check if function has only pass statement."""
        if len(node.body) == 1 and isinstance(node.body[0], ast.Pass):
            return True
        return False

    def _has_not_implemented_error(self, node: ast.FunctionDef) -> bool:
        """Check if function raises NotImplementedError."""
        for stmt in node.body:
            if isinstance(stmt, ast.Raise):
                if isinstance(stmt.exc, ast.Call):
                    if isinstance(stmt.exc.func, ast.Name) and stmt.exc.func.id == "NotImplementedError":
                        return True
        return False

    def _is_abstract_method(self, node: ast.FunctionDef) -> bool:
        """Check if method is abstract."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "abstractmethod":
                return True
        return False

    def _analyze_import(self, node: ast.Import, file_path: Path) -> None:
        """Analyze import statement."""
        for alias in node.names:
            self.code_map["imports"][f"{file_path}:{alias.name}"] = {
                "name": alias.name,
                "file": str(file_path),
                "line": node.lineno,
                "type": "import"
            }

    def _analyze_import_from(self, node: ast.ImportFrom, file_path: Path) -> None:
        """Analyze import from statement."""
        module = node.module or ""
        for alias in node.names:
            self.code_map["imports"][f"{file_path}:{alias.name}"] = {
                "name": alias.name,
                "file": str(file_path),
                "line": node.lineno,
                "type": "import_from",
                "module": module
            }

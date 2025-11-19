"""
Issue detector for the code mapper.

This module contains issue detection functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import ast
from typing import Dict, List, Any
from pathlib import Path


class IssueDetector:
    """Issue detection functionality."""

    def __init__(self, issues: Dict[str, List[Any]]):
        """Initialize issue detector."""
        self.issues = issues

    def check_method_issues(
        self, node: ast.FunctionDef, file_path: str, class_name: str = None
    ) -> None:
        """Check for method-specific issues."""
        # Check for Any type usage in parameters
        self._check_any_type_usage(node, file_path, class_name)
        # Check for generic Exception usage
        self._check_generic_exception_usage(node, file_path, class_name)

    def _check_any_type_usage(
        self, node: ast.FunctionDef, file_path: str, class_name: str = None
    ) -> None:
        """Check for Any type usage in function parameters and return type."""
        # Check return type annotation
        if node.returns:
            if isinstance(node.returns, ast.Name) and node.returns.id == "Any":
                self.issues["any_type_usage"].append(
                    {
                        "file": file_path,
                        "class": class_name,
                        "method": node.name,
                        "type": "return_type",
                        "line": node.lineno,
                        "description": "Return type is Any",
                    }
                )
            elif isinstance(node.returns, ast.Attribute) and node.returns.attr == "Any":
                self.issues["any_type_usage"].append(
                    {
                        "file": file_path,
                        "class": class_name,
                        "method": node.name,
                        "type": "return_type",
                        "line": node.lineno,
                        "description": "Return type is Any",
                    }
                )

        # Check parameter type annotations
        for arg in node.args.args:
            if arg.annotation:
                if isinstance(arg.annotation, ast.Name) and arg.annotation.id == "Any":
                    self.issues["any_type_usage"].append(
                        {
                            "file": file_path,
                            "class": class_name,
                            "method": node.name,
                            "type": "parameter",
                            "parameter": arg.arg,
                            "line": node.lineno,
                            "description": f"Parameter '{arg.arg}' has Any type",
                        }
                    )
                elif (
                    isinstance(arg.annotation, ast.Attribute)
                    and arg.annotation.attr == "Any"
                ):
                    self.issues["any_type_usage"].append(
                        {
                            "file": file_path,
                            "class": class_name,
                            "method": node.name,
                            "type": "parameter",
                            "parameter": arg.arg,
                            "line": node.lineno,
                            "description": f"Parameter '{arg.arg}' has Any type",
                        }
                    )

    def _check_generic_exception_usage(
        self, node: ast.FunctionDef, file_path: str, class_name: str = None
    ) -> None:
        """Check for generic Exception usage in function body."""
        for stmt in node.body:
            self._check_statement_for_generic_exception(
                stmt, file_path, class_name, node.name
            )

    def _check_statement_for_generic_exception(
        self, stmt: ast.AST, file_path: str, class_name: str, method_name: str
    ) -> None:
        """Recursively check statement for generic Exception usage."""
        if isinstance(stmt, ast.Try):
            # Check except clauses
            for handler in stmt.handlers:
                if handler.type:
                    if (
                        isinstance(handler.type, ast.Name)
                        and handler.type.id == "Exception"
                    ):
                        self.issues["generic_exception_usage"].append(
                            {
                                "file": file_path,
                                "class": class_name,
                                "method": method_name,
                                "line": handler.lineno,
                                "type": "except_clause",
                                "description": "Generic Exception caught without specific exception type",
                            }
                        )
                    elif (
                        isinstance(handler.type, ast.Attribute)
                        and handler.type.attr == "Exception"
                    ):
                        self.issues["generic_exception_usage"].append(
                            {
                                "file": file_path,
                                "class": class_name,
                                "method": method_name,
                                "line": handler.lineno,
                                "type": "except_clause",
                                "description": "Generic Exception caught without specific exception type",
                            }
                        )
                else:
                    # Bare except clause
                    self.issues["generic_exception_usage"].append(
                        {
                            "file": file_path,
                            "class": class_name,
                            "method": method_name,
                            "line": handler.lineno,
                            "type": "bare_except",
                            "description": "Bare except clause without exception type",
                        }
                    )
            # Check for raise Exception within try block
            for stmt_in_try in stmt.body:
                self._check_statement_for_generic_exception(
                    stmt_in_try, file_path, class_name, method_name
                )
        elif isinstance(stmt, ast.Raise):
            # Check if raising generic Exception
            if stmt.exc:
                if isinstance(stmt.exc, ast.Name) and stmt.exc.id == "Exception":
                    self.issues["generic_exception_usage"].append(
                        {
                            "file": file_path,
                            "class": class_name,
                            "method": method_name,
                            "line": stmt.lineno,
                            "type": "raise_exception",
                            "description": "Raising generic Exception instead of specific exception",
                        }
                    )
                elif isinstance(stmt.exc, ast.Call):
                    if (
                        isinstance(stmt.exc.func, ast.Name)
                        and stmt.exc.func.id == "Exception"
                    ):
                        self.issues["generic_exception_usage"].append(
                            {
                                "file": file_path,
                                "class": class_name,
                                "method": method_name,
                                "line": stmt.lineno,
                                "type": "raise_exception",
                                "description": "Raising generic Exception instead of specific exception",
                            }
                        )

        # Recursively check nested statements
        if hasattr(stmt, "body") and isinstance(stmt.body, list):
            for nested_stmt in stmt.body:
                self._check_statement_for_generic_exception(
                    nested_stmt, file_path, class_name, method_name
                )

    def check_imports_in_middle(self, file_path: str, line_number: int) -> None:
        """Check for imports in the middle of files."""
        self.issues["imports_in_middle"].append(
            {"file": file_path, "line": line_number}
        )

"""
Code Validator for LLM-generated Python scripts.

This module provides security validation for dynamically generated code
to prevent execution of malicious or dangerous operations.
"""

import ast
import logging
from typing import Tuple, Optional, List, Set

logger = logging.getLogger(__name__)


class CodeValidator:
    """
    Validates Python code for security risks before execution.
    
    Uses AST parsing to detect:
    - Dangerous imports (os.system, eval, exec, subprocess)
    - Forbidden function calls
    - Syntax errors
    - Compilation issues
    """
    
    # Allowed imports for data engineering tasks
    ALLOWED_IMPORTS = {
        # Data processing
        'pandas', 'numpy', 'pyarrow',
        # AWS/S3
        'boto3', 'botocore',
        # HTTP requests
        'requests', 'urllib', 'urllib3', 'http',
        # Standard library - safe subset
        'json', 'csv', 'datetime', 'time', 're', 'typing',
        'io', 'pathlib', 'collections', 'itertools', 'functools',
        'math', 'statistics', 'decimal', 'fractions',
        'hashlib', 'uuid', 'base64',
        # Logging
        'logging',
    }
    
    # Dangerous imports that should be blocked
    DANGEROUS_IMPORTS = {
        'os.system', 'subprocess', 'eval', 'exec', 'compile',
        '__import__', 'importlib',
        'socket', 'telnetlib', 'ftplib', 'smtplib',
        'pickle', 'shelve', 'marshal',
        'ctypes', 'cffi',
        'pty', 'tty',
        'code', 'codeop',
    }
    
    # Dangerous built-in functions
    DANGEROUS_BUILTINS = {
        'eval', 'exec', 'compile', '__import__',
        'open',  # We want controlled file access only
        'input',  # No interactive input in automated scripts
    }
    
    def __init__(self):
        """Initialize the code validator."""
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.suggestions: List[str] = []
    
    def validate(self, code: str) -> Tuple[bool, Optional[str], List[str]]:
        """
        Validate Python code for security and syntax.
        
        Args:
            code: Python source code to validate
            
        Returns:
            Tuple of (is_valid, error_message, suggestions)
            - is_valid: True if code passes all validation checks
            - error_message: Description of validation failure (None if valid)
            - suggestions: List of suggestions to fix issues
        """
        self.errors = []
        self.warnings = []
        self.suggestions = []
        
        # Step 1: Syntax validation
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            error_msg = f"Syntax error at line {e.lineno}: {e.msg}"
            self.errors.append(error_msg)
            self.suggestions.append("Fix the syntax error and try again")
            return False, error_msg, self.suggestions
        except Exception as e:
            error_msg = f"Failed to parse code: {str(e)}"
            self.errors.append(error_msg)
            return False, error_msg, self.suggestions
        
        # Step 2: Compile-time validation
        try:
            compile(code, '<string>', 'exec')
        except Exception as e:
            error_msg = f"Compilation error: {str(e)}"
            self.errors.append(error_msg)
            self.suggestions.append("Ensure all variables and functions are properly defined")
            return False, error_msg, self.suggestions
        
        # Step 3: AST-based security checks
        self._check_imports(tree)
        self._check_function_calls(tree)
        self._check_dangerous_operations(tree)
        
        # Compile results
        if self.errors:
            error_msg = "; ".join(self.errors)
            logger.warning(f"Code validation failed: {error_msg}")
            return False, error_msg, self.suggestions
        
        if self.warnings:
            logger.info(f"Code validation warnings: {'; '.join(self.warnings)}")
        
        logger.info("Code validation passed")
        return True, None, []
    
    def _check_imports(self, tree: ast.AST) -> None:
        """Check for dangerous or disallowed imports."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self._validate_import(alias.name)
            
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    full_name = f"{module}.{alias.name}" if module else alias.name
                    self._validate_import(full_name)
                    self._validate_import(module)
    
    def _validate_import(self, import_name: str) -> None:
        """Validate a single import name."""
        # Check if it's a dangerous import
        if import_name in self.DANGEROUS_IMPORTS:
            self.errors.append(f"Dangerous import detected: '{import_name}'")
            self.suggestions.append(f"Remove '{import_name}' - this import is not allowed for security reasons")
            return
        
        # Check if any dangerous import is a prefix
        for dangerous in self.DANGEROUS_IMPORTS:
            if import_name.startswith(dangerous):
                self.errors.append(f"Dangerous import detected: '{import_name}' (matches '{dangerous}')")
                self.suggestions.append(f"Remove '{import_name}' - this import is not allowed")
                return
        
        # Check if it's in the allowed list (base module name)
        base_module = import_name.split('.')[0]
        if base_module not in self.ALLOWED_IMPORTS:
            self.warnings.append(f"Uncommon import: '{import_name}'")
            self.suggestions.append(f"Verify that '{import_name}' is necessary and safe")
    
    def _check_function_calls(self, tree: ast.AST) -> None:
        """Check for dangerous function calls."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node.func)
                
                if func_name in self.DANGEROUS_BUILTINS:
                    self.errors.append(f"Dangerous function call detected: '{func_name}()'")
                    self.suggestions.append(f"Remove '{func_name}()' - this function is not allowed")
                
                # Check for subprocess-like patterns
                if 'system' in func_name.lower() or 'popen' in func_name.lower():
                    self.errors.append(f"Potentially dangerous function call: '{func_name}()'")
                    self.suggestions.append("Use boto3 or requests instead of system calls")
    
    def _check_dangerous_operations(self, tree: ast.AST) -> None:
        """Check for other dangerous operations."""
        for node in ast.walk(tree):
            # Check for dynamic code execution patterns
            if isinstance(node, ast.Expr):
                if isinstance(node.value, ast.Call):
                    func_name = self._get_function_name(node.value.func)
                    if func_name in ['eval', 'exec', 'compile']:
                        self.errors.append(f"Dynamic code execution detected: '{func_name}'")
                        self.suggestions.append("Remove dynamic code execution for security")
            
            # Check for file operations (we want controlled access)
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node.func)
                if func_name == 'open':
                    # Allow open() but warn about it
                    self.warnings.append("File operation detected: open()")
                    self.suggestions.append("Ensure file paths are validated and safe")
    
    def _get_function_name(self, node: ast.AST) -> str:
        """Extract function name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # For calls like os.system, get the full name
            parts = []
            current = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return '.'.join(reversed(parts))
        return ''
    
    def get_validation_report(self) -> str:
        """Generate a detailed validation report."""
        report = []
        
        if self.errors:
            report.append("ERRORS:")
            for error in self.errors:
                report.append(f"  ❌ {error}")
        
        if self.warnings:
            report.append("\nWARNINGS:")
            for warning in self.warnings:
                report.append(f"  ⚠️  {warning}")
        
        if self.suggestions:
            report.append("\nSUGGESTIONS:")
            for suggestion in self.suggestions:
                report.append(f"  💡 {suggestion}")
        
        return "\n".join(report) if report else "✅ Code validation passed"


# Singleton instance
code_validator = CodeValidator()

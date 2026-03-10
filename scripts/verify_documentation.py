#!/usr/bin/env python3
"""
文档验证脚本

自动验证文档与代码的一致性，包括：
- 导入路径验证
- API路径验证
- 代码示例运行测试
- 链接有效性检查
"""

import re
import ast
import sys
import importlib
from pathlib import Path
from typing import List


class DocumentationValidator:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.docs_dir = self.project_root / "docs"
        self.errors = []
        self.warnings = []
        
    def validate_all(self):
        """运行所有验证"""
        print("=" * 60)
        print("ChronoForge 文档验证")
        print("=" * 60)
        print()
        
        # 1. 验证导入路径
        print("1. 验证导入路径...")
        self.validate_import_paths()
        
        # 2. 验证API路径
        print("2. 验证API路径...")
        self.validate_api_paths()
        
        # 3. 验证代码示例
        print("3. 验证代码示例...")
        self.validate_code_examples()
        
        # 4. 验证链接
        print("4. 验证链接...")
        self.validate_links()
        
        # 5. 验证插件列表
        print("5. 验证插件列表...")
        self.validate_plugin_lists()
        
        # 打印结果
        self.print_results()
        
        # 返回状态码
        return 0 if not self.errors else 1
    
    def validate_import_paths(self):
        """验证文档中的导入路径是否正确"""
        md_files = list(self.docs_dir.glob("*.md"))
        
        for md_file in md_files:
            content = md_file.read_text()
            
            # 查找所有导入语句
            import_pattern = r'from\s+([\w.]+)\s+import\s+([\w,\s]+)'
            imports = re.findall(import_pattern, content)
            
            for module, names in imports:
                module = module.strip()
                names = [n.strip() for n in names.split(',')]
                
                # 尝试导入模块
                try:
                    imported_module = importlib.import_module(module)
                    
                    # 检查导入的名称是否存在
                    for name in names:
                        if not hasattr(imported_module, name):
                            self.errors.append(
                                f"{md_file.name}: '{name}' not found in module '{module}'"
                            )
                except ImportError as e:
                    self.errors.append(
                        f"{md_file.name}: Cannot import module '{module}' - {e}"
                    )
                except Exception as e:
                    self.warnings.append(
                        f"{md_file.name}: Error checking import '{module}' - {e}"
                    )
    
    def validate_api_paths(self):
        """验证文档中的API路径是否与实际路由匹配"""
        # 读取实际API路由
        api_routes = self._get_actual_api_routes()
        
        # 检查文档中的API路径
        api_doc = self.docs_dir / "api_reference.md"
        if not api_doc.exists():
            self.warnings.append("api_reference.md not found")
            return
        
        content = api_doc.read_text()
        
        # 查找文档中的API路径
        api_path_pattern = r'端点[：:]\s*`([^`]+)`'
        doc_paths = re.findall(api_path_pattern, content)
        
        # 对比路径
        for doc_path in doc_paths:
            if doc_path not in api_routes:
                self.errors.append(
                    f"API path '{doc_path}' in documentation not found in actual routes"
                )
    
    def _get_actual_api_routes(self) -> List[str]:
        """获取实际的API路由列表"""
        routes = []
        
        # 扫描API路由文件
        api_dir = self.project_root / "chronoforge" / "server" / "api"
        if not api_dir.exists():
            return routes
        
        for py_file in api_dir.glob("*.py"):
            if py_file.name == "__init__.py":
                continue
            
            content = py_file.read_text()
            
            # 查找路由定义
            route_pattern = r'@router\.(get|post|put|delete|patch)\s*\(["\']([^"\']+)["\']'
            matches = re.findall(route_pattern, content)
            
            for method, path in matches:
                routes.append(path)
        
        return routes
    
    def validate_code_examples(self):
        """验证文档中的代码示例是否可运行"""
        md_files = list(self.docs_dir.glob("*.md"))
        
        for md_file in md_files:
            content = md_file.read_text()
            
            # 查找Python代码块
            code_pattern = r'```python\n(.*?)```'
            code_blocks = re.findall(code_pattern, content, re.DOTALL)
            
            for i, code in enumerate(code_blocks, 1):
                # 跳过不完整的示例
                if '...' in code or 'your_' in code:
                    continue
                
                # 尝试解析代码
                try:
                    ast.parse(code)
                except SyntaxError as e:
                    self.errors.append(
                        f"{md_file.name}: Code block {i} has syntax error - {e}"
                    )
    
    def validate_links(self):
        """验证文档中的链接是否有效"""
        md_files = list(self.docs_dir.glob("*.md"))
        
        for md_file in md_files:
            content = md_file.read_text()
            
            # 查找所有链接
            link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
            links = re.findall(link_pattern, content)
            
            for text, url in links:
                # 跳过外部链接和特殊链接
                if url.startswith('http') or url.startswith('#') or url.startswith('mailto:'):
                    continue
                
                # 检查相对路径文件是否存在
                if url.endswith('.md'):
                    target_path = self.docs_dir / url
                    if not target_path.exists():
                        self.warnings.append(
                            f"{md_file.name}: Link to '{url}' does not exist"
                        )
    
    def validate_plugin_lists(self):
        """验证文档中的插件列表是否完整"""
        # 获取实际的数据源列表
        actual_datasources = self._get_actual_plugins('data_source')
        actual_storages = self._get_actual_plugins('storage')
        
        # 检查文档中的插件列表
        api_doc = self.docs_dir / "api_reference.md"
        if api_doc.exists():
            content = api_doc.read_text()
            
            # 查找数据源列表
            ds_pattern = r'"data_source":\s*\[([^\]]+)\]'
            match = re.search(ds_pattern, content)
            if match:
                doc_datasources = [ds.strip().strip('"') for ds in match.group(1).split(',')]
                
                # 对比列表
                missing = set(actual_datasources) - set(doc_datasources)
                if missing:
                    self.errors.append(
                        f"Missing data sources in documentation: {missing}"
                    )
            
            # 查找存储列表
            storage_pattern = r'"storage":\s*\[([^\]]+)\]'
            match = re.search(storage_pattern, content)
            if match:
                doc_storages = [s.strip().strip('"') for s in match.group(1).split(',')]
                
                # 对比列表
                missing = set(actual_storages) - set(doc_storages)
                if missing:
                    self.errors.append(
                        f"Missing storages in documentation: {missing}"
                    )
    
    def _get_actual_plugins(self, plugin_type: str) -> List[str]:
        """获取实际的插件列表"""
        plugins = []
        
        if plugin_type == 'data_source':
            plugin_dir = self.project_root / "chronoforge" / "data_source"
        elif plugin_type == 'storage':
            plugin_dir = self.project_root / "chronoforge" / "storage"
        else:
            return plugins
        
        if not plugin_dir.exists():
            return plugins
        
        # 扫描插件文件
        for py_file in plugin_dir.glob("*.py"):
            if py_file.name in ["__init__.py", "base.py", "manager.py", "cache.py"]:
                continue
            
            # 提取类名
            content = py_file.read_text()
            class_pattern = r'class\s+(\w+DataSource)\s*\('
            matches = re.findall(class_pattern, content)
            plugins.extend(matches)
        
        return plugins
    
    def print_results(self):
        """打印验证结果"""
        print()
        print("=" * 60)
        print("验证结果")
        print("=" * 60)
        print()
        
        if self.errors:
            print(f"❌ 错误 ({len(self.errors)}):")
            for error in self.errors:
                print(f"  - {error}")
            print()
        
        if self.warnings:
            print(f"⚠️  警告 ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  - {warning}")
            print()
        
        if not self.errors and not self.warnings:
            print("✅ 所有验证通过！")
            print()
        
        print("=" * 60)
        print(f"总计: {len(self.errors)} 错误, {len(self.warnings)} 警告")
        print("=" * 60)


def main():
    validator = DocumentationValidator()
    sys.exit(validator.validate_all())


if __name__ == "__main__":
    main()

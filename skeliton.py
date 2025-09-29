import json
import pandas as pd
from typing import Dict, List, Any, Union
from abc import ABC, abstractmethod
import os

class Node(ABC):
    """Base abstract class for all processing nodes"""
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def process(self):
        """Abstract method that each node must implement"""
        pass

class InputNode(Node):
    """INPUT Node: Handles Excel file input and data loading"""
    
    def __init__(self, excel_file_path: str):
        super().__init__("InputNode")
        self.excel_file_path = excel_file_path
        self.data = None
        self.columns = []
        self.rows = []
    
    def process(self) -> pd.DataFrame:
        """Load Excel file and return DataFrame"""
        try:
            self.data = pd.read_excel(self.excel_file_path)
            self.columns = self.data.columns.tolist()
            self.rows = self.data.values.tolist()
            print(f"Successfully loaded Excel: {len(self.rows)} rows, {len(self.columns)} columns")
            return self.data
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return None
    
    def get_data(self) -> pd.DataFrame:
        """Get loaded data or process if not already loaded"""
        return self.data if self.data is not None else self.process()
    
    def get_columns(self) -> List[str]:
        """Get column names"""
        return self.columns
    
    def get_row_count(self) -> int:
        """Get number of rows"""
        return len(self.rows)

class RuleForWriterNode(Node):
    """RULE FOR WRITER Node: Manages conversion rules from JSON file"""
    
    def __init__(self, rules_file_path: str):
        super().__init__("RuleForWriterNode")
        self.rules_file_path = rules_file_path
        self.rules = None
    
    def process(self) -> Dict[str, Any]:
        """Load rules from JSON file"""
        try:
            with open(self.rules_file_path, 'r', encoding='utf-8') as file:
                self.rules = json.load(file)
            print(f"Successfully loaded rules from: {self.rules_file_path}")
            return self.rules
        except FileNotFoundError:
            print("Rules file not found, using default rules")
            self.rules = self._get_default_rules()
            return self.rules
        except Exception as e:
            print(f"Error loading rules: {e}")
            return None
    
    def _get_default_rules(self) -> Dict[str, Any]:
        """Default rules structure"""
        return {
            "patterns": {
                "text_pattern": {
                    "pi_code": "MES_REVENGG_TESTRUN",
                    "section_prefix": "SECTION",
                    "label_prefix": "LABEL_",
                    "code_suffix": "_TextPara",
                    "kind_mapping": {
                        "text": "TEXT",
                        "number": "NUMBER",
                        "boolean": "BOOLEAN"
                    }
                }
            }
        }
    
    def get_rules(self) -> Dict[str, Any]:
        """Get loaded rules"""
        return self.rules if self.rules is not None else self.process()
    
    def get_rule_by_pattern(self, pattern_key: str) -> Dict[str, Any]:
        """Get specific rule pattern"""
        if self.rules and "patterns" in self.rules:
            return self.rules["patterns"].get(pattern_key, {})
        return {}

class JsonWriterNode(Node):
    """JSON WRITER Node: Main processing unit that converts Excel to JSON using rules"""
    
    def __init__(self, input_node: InputNode, rule_node: RuleForWriterNode):
        super().__init__("JsonWriterNode")
        self.input_node = input_node
        self.rule_node = rule_node
        self.output_json = None
    
    def analyze_data_pattern(self, data: pd.DataFrame) -> str:
        """Analyze Excel data to determine which rule pattern to use"""
        columns = data.columns.tolist()
        column_str = ' '.join([col.lower() for col in columns])
        
        # Pattern recognition logic
        if 'text' in column_str or 'name' in column_str:
            return "text_pattern"
        elif 'section' in column_str:
            return "section_pattern"
        else:
            return "text_pattern"  # default
    
    def determine_data_type(self, value: Any) -> str:
        """Determine data type for 'kind' field"""
        if pd.isna(value):
            return "TEXT"
        elif isinstance(value, str):
            return "TEXT"
        elif isinstance(value, (int, float)):
            return "NUMBER"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "TEXT"
    
    def process_sections(self, data: pd.DataFrame, rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data into sections based on rules"""
        sections = []
        section_prefix = rules.get("section_prefix", "SECTION")
        label_prefix = rules.get("label_prefix", "LABEL_")
        code_suffix = rules.get("code_suffix", "_Code")
        
        for index, row in data.iterrows():
            section = {
                "title": f"{section_prefix} {index + 1}",
                "rows": []
            }
            
            # Process each column in the row
            for col_name, cell_value in row.items():
                if pd.notna(cell_value):
                    row_item = {
                        "label": f"{label_prefix}{col_name.upper()}",
                        "kind": self.determine_data_type(cell_value),
                        "code": f"{col_name}{code_suffix}",
                        "html_overrides": {
                            "value": f"[GENAI_REVENGG_{str(cell_value).upper().replace(' ', '_')}]"
                        }
                    }
                    section["rows"].append(row_item)
            
            sections.append(section)
        
        return sections
    
    def process(self) -> Dict[str, Any]:
        """Main processing method"""
        # Get data from input node
        data = self.input_node.get_data()
        if data is None:
            print("No data available from input node")
            return None
        
        # Get rules from rule node
        rules = self.rule_node.get_rules()
        if rules is None:
            print("No rules available from rule node")
            return None
        
        # Analyze data pattern
        pattern = self.analyze_data_pattern(data)
        print(f"Identified pattern: {pattern}")
        
        # Get specific rules for this pattern
        pattern_rules = self.rule_node.get_rule_by_pattern(pattern)
        
        # Build output JSON structure
        self.output_json = {
            "pi_code": pattern_rules.get("pi_code", "DEFAULT_CODE"),
            "sections": self.process_sections(data, pattern_rules)
        }
        
        print(f"Successfully processed {len(self.output_json['sections'])} sections")
        return self.output_json
    
    def get_output(self) -> Dict[str, Any]:
        """Get processed JSON output"""
        return self.output_json if self.output_json is not None else self.process()

class OutputNode(Node):
    """OUTPUT Node: Handles JSON file output"""
    
    def __init__(self, json_writer_node: JsonWriterNode, output_file_path: str):
        super().__init__("OutputNode")
        self.json_writer_node = json_writer_node
        self.output_file_path = output_file_path
    
    def process(self) -> bool:
        """Write JSON output to file"""
        try:
            output_data = self.json_writer_node.get_output()
            if output_data is None:
                print("No data to write")
                return False
            
            with open(self.output_file_path, 'w', encoding='utf-8') as file:
                json.dump(output_data, file, indent=4, ensure_ascii=False)
            
            print(f"Successfully wrote JSON to: {self.output_file_path}")
            return True
        except Exception as e:
            print(f"Error writing JSON file: {e}")
            return False

class ExcelToJsonConverter:
    """Main converter class that orchestrates all nodes"""
    
    def __init__(self, excel_file_path: str, rules_file_path: str, output_file_path: str = None):
        self.input_node = InputNode(excel_file_path)
        self.rule_node = RuleForWriterNode(rules_file_path)
        self.json_writer_node = JsonWriterNode(self.input_node, self.rule_node)
        # If output_file_path is not provided, use default in current directory
        if output_file_path is None:
            output_file_path = os.path.join(os.getcwd(), "converted_output.json")
        self.output_node = OutputNode(self.json_writer_node, output_file_path)
    
    def convert(self) -> bool:
        """Execute the complete conversion process"""
        print("Starting Excel to JSON conversion...")
        return self.output_node.process()
    
    def get_preview(self) -> Dict[str, Any]:
        """Get preview of conversion output without writing file"""
        return self.json_writer_node.get_output()

# Example usage
if __name__ == "__main__":
    converter = ExcelToJsonConverter(
        excel_file_path="input_data.xlsx",
        rules_file_path="conversion_rules.json"
        # output_file_path is omitted, will use default
    )
    
    # Get preview
    preview = converter.get_preview()
    print("Preview:", json.dumps(preview, indent=2))
    
    # Execute conversion
    success = converter.convert()
    print(f"Conversion {'successful' if success else 'failed'}")
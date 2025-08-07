from pathlib import Path
import csv
import re

def create_index():
    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()
    saved_dir = script_dir.parent / 'saved'
    index_file = script_dir / 'index.csv'
    
    # Check if saved directory exists
    if not saved_dir.exists():
        print(f"Error: {saved_dir} directory not found")
        return
    
    # Get all .py files in saved directory
    py_files = list(saved_dir.glob('*.py'))
    
    if not py_files:
        print(f"No .py files found in {saved_dir} directory")
        return
    
    # Validate strategy names and separate valid/invalid ones
    strategy_pattern = re.compile(r'^\d{6}-\d+-\d+$')
    valid_strategies = []
    invalid_strategies = []
    
    for py_file in py_files:
        strategy_name = py_file.stem
        if strategy_pattern.match(strategy_name):
            valid_strategies.append(py_file)
        else:
            invalid_strategies.append(py_file)
            print(f"Warning: Strategy '{strategy_name}' does not match expected format YYMMDD-X-Y")
    
    # Sort valid strategies in reverse order (newest first)
    valid_strategies.sort(key=lambda x: x.stem, reverse=True)
    
    # Sort invalid strategies lexicographically
    invalid_strategies.sort(key=lambda x: x.stem)
    
    # Combine lists: invalid first, then valid
    all_files = invalid_strategies + valid_strategies
    
    # Create index.csv
    with open(Path(index_file), 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['strategy_name', 'explanation'])
        
        for py_file in all_files:
            strategy_name = py_file.stem  # Remove .py extension
            
            # Read the file to extract explanation
            explanation = ""
            
            try:
                with open(Path(py_file), 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Look for _explanation variable
                    explanation_match = re.search(r'_explanation\s*=\s*r?"""(.*?)"""', content, re.DOTALL)
                    if explanation_match:
                        explanation = explanation_match.group(1).strip()
                        # Clean up the explanation (remove extra whitespace)
                        explanation = ' '.join(explanation.split())
                    else:
                        # If no _explanation found, try to get the first docstring or comment
                        docstring_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
                        if docstring_match:
                            explanation = docstring_match.group(1).strip()
                            explanation = ' '.join(explanation.split())
                        else:
                            explanation = "No explanation found"
                            
            except Exception as e:
                explanation = f"Error reading file: {str(e)}"
            
            writer.writerow([strategy_name, explanation])
    
    print(f"Index created successfully: {index_file}")
    print(f"Indexed {len(py_files)} strategy files")

if __name__ == "__main__":
    create_index()
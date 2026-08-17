import os
import sys
import io
from unittest.mock import MagicMock, patch
from blobforge.ingestor import ingest

def test_ingest_logging():
    test_file = "test_ingest.pdf"
    content = b"%PDF-1.4 test content"
    with open(test_file, "wb") as f:
        f.write(content)
    
    try:
        # Mock CoordinatorClient to avoid network calls
        with patch('blobforge.ingestor.CoordinatorClient') as MockCoordinatorClient:
            coordinator = MockCoordinatorClient.return_value
            coordinator.available = True
            coordinator.raw_upload_url.return_value = {"url": "https://s3.example/raw", "already_exists": False, "headers": {}}
            coordinator.enqueue.return_value = {'status': 'todo', 'priority': '3_normal'}
            coordinator.get_job.return_value = {}
            
            # Capture stdout
            captured_output = io.StringIO()
            sys.stdout = captured_output
            
            # 1. First ingest - should compute hash
            print("--- FIRST INGEST ---", file=sys.stderr)
            ingest([test_file])
            
            sys.stdout = sys.__stdout__
            output1 = captured_output.getvalue()
            print(output1, file=sys.stderr)
            assert "Computing hash..." in output1
            
            # 2. Second ingest - should use cache
            captured_output = io.StringIO()
            sys.stdout = captured_output
            
            print("--- SECOND INGEST ---", file=sys.stderr)
            ingest([test_file])
            
            sys.stdout = sys.__stdout__
            output2 = captured_output.getvalue()
            print(output2, file=sys.stderr)
            assert "Computing hash..." not in output2
            assert "Found: test_ingest.pdf ->" in output2
            
            print("Ingest logging test passed!")
            
    finally:
        sys.stdout = sys.__stdout__
        if os.path.exists(test_file):
            os.remove(test_file)

if __name__ == "__main__":
    test_ingest_logging()

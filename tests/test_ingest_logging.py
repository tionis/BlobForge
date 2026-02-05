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
        # Mock S3Client to avoid network calls
        with patch('blobforge.ingestor.S3Client') as MockS3Client:
            mock_s3 = MockS3Client.return_value
            mock_s3.get_manifest.return_value = {'entries': {}}
            mock_s3.exists.return_value = False
            
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
            # Ensure it's not in manifest so it doesn't skip too early
            mock_s3.get_manifest.return_value = {'entries': {}}
            
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

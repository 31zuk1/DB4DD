import shutil
import logging
import hashlib
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

class VaultSynchronizer:
    """Synchronizes daily vault outputs to the master vault."""
    
    def __init__(self, master_dir: Path):
        self.master_dir = master_dir
        self.master_dir.mkdir(parents=True, exist_ok=True)
        
    def sync(self, source_dir: Path) -> Tuple[int, int]:
        """
        Sync files from source_dir to master_dir.
        Returns: (updated_count, new_count)
        """
        logger.info(f"Syncing from {source_dir} to {self.master_dir}")
        
        updated_count = 0
        new_count = 0
        
        # Walk through source directory
        for source_path in source_dir.rglob('*'):
            if not source_path.is_file():
                continue
                
            # Skip hidden files
            if source_path.name.startswith('.'):
                continue
                
            # Calculate relative path
            rel_path = source_path.relative_to(source_dir)
            target_path = self.master_dir / rel_path
            
            # Ensure target directory exists
            target_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if copy is needed
            if not target_path.exists():
                self._copy_file(source_path, target_path)
                new_count += 1
                logger.info(f"  [NEW] {rel_path}")
            elif self._files_differ(source_path, target_path):
                self._copy_file(source_path, target_path)
                updated_count += 1
                logger.info(f"  [UPD] {rel_path}")
                
        logger.info(f"Sync complete. New: {new_count}, Updated: {updated_count}")
        return updated_count, new_count
        
    def _copy_file(self, src: Path, dst: Path):
        """Copy file with metadata."""
        shutil.copy2(src, dst)
        
    def _files_differ(self, file1: Path, file2: Path) -> bool:
        """Check if files are different based on size and content hash."""
        # Fast check: size
        if file1.stat().st_size != file2.stat().st_size:
            return True
            
        # Slow check: hash
        return self._get_file_hash(file1) != self._get_file_hash(file2)
        
    def _get_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()

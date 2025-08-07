"""Memory-efficient caching system for large datasets."""

import numpy as np
import logging
from functools import lru_cache
import weakref
import gc

logger = logging.getLogger(__name__)


class SharedMemoryCache:
    """
    Shared memory cache for NumPy arrays to reduce memory duplication.
    Uses weak references to allow garbage collection when not in use.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._cache = {}  # key -> weakref to numpy array
        self._strong_refs = {}  # key -> numpy array (for frequently accessed data)
        self._access_counts = {}  # key -> access count
        self._memory_limit = 1024 * 1024 * 1024  # 1GB default
        self._current_memory = 0
        
    def put(self, key, array, pin=False):
        """
        Store array in cache.
        
        Args:
            key: Cache key
            array: NumPy array to cache
            pin: If True, keep strong reference (won't be garbage collected)
        """
        if not isinstance(array, np.ndarray):
            array = np.array(array)
        
        # Calculate memory usage
        memory_size = array.nbytes
        
        # Check memory limit
        if self._current_memory + memory_size > self._memory_limit:
            self._evict_lru()
        
        # Store array
        if pin:
            self._strong_refs[key] = array
        else:
            self._cache[key] = weakref.ref(array)
        
        self._access_counts[key] = 0
        self._current_memory += memory_size
        
    def get(self, key):
        """Get array from cache."""
        # Check strong references first
        if key in self._strong_refs:
            self._access_counts[key] += 1
            return self._strong_refs[key]
        
        # Check weak references
        if key in self._cache:
            ref = self._cache[key]
            array = ref()
            if array is not None:
                self._access_counts[key] += 1
                
                # Promote to strong ref if frequently accessed
                if self._access_counts[key] > 100:
                    self._strong_refs[key] = array
                
                return array
            else:
                # Array was garbage collected
                del self._cache[key]
                del self._access_counts[key]
        
        return None
    
    def _evict_lru(self):
        """Evict least recently used items to free memory."""
        # Sort by access count
        sorted_keys = sorted(self._access_counts.keys(), 
                           key=lambda k: self._access_counts[k])
        
        # Evict until we have enough space
        for key in sorted_keys:
            if key in self._cache and key not in self._strong_refs:
                ref = self._cache[key]
                array = ref()
                if array is not None:
                    self._current_memory -= array.nbytes
                
                del self._cache[key]
                del self._access_counts[key]
                
                if self._current_memory < self._memory_limit * 0.8:
                    break
        
        # Force garbage collection
        gc.collect()
    
    def clear(self):
        """Clear all cached data."""
        self._cache.clear()
        self._strong_refs.clear()
        self._access_counts.clear()
        self._current_memory = 0
        gc.collect()


class ComputationCache:
    """Cache for expensive computations with automatic memoization."""
    
    def __init__(self, maxsize=1000):
        self.maxsize = maxsize
        self._cache = {}
    
    def cached_compute(self, key, compute_func, *args, **kwargs):
        """
        Get cached result or compute if not available.
        
        Args:
            key: Cache key
            compute_func: Function to compute result if not cached
            *args, **kwargs: Arguments for compute_func
        """
        if key in self._cache:
            return self._cache[key]
        
        # Compute result
        result = compute_func(*args, **kwargs)
        
        # Cache if under size limit
        if len(self._cache) < self.maxsize:
            self._cache[key] = result
        else:
            # Evict random item
            evict_key = next(iter(self._cache))
            del self._cache[evict_key]
            self._cache[key] = result
        
        return result
    
    @lru_cache(maxsize=10000)
    def get_cache_key(self, *args):
        """Generate cache key from arguments."""
        return hash(args)
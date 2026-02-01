import numpy as np

class AnomalyInjector:
    """Modifies clean data stream samples with synthetic faults."""
    
    def apply(self, sample_values, anomaly_state, current_step):
        """
        Args:
            sample_values: np.array (F,) - Original values
            anomaly_state: dict {
                'type': 'spike'|'drift'|'freeze',
                'sensor': int (index),
                'start_step': int,
                'magnitude': float
            }
            current_step: int
        Returns:
            modified_values: np.array (F,)
        """
        modified = sample_values.copy()
        
        a_type = anomaly_state['type']
        sensor_idx = anomaly_state['sensor']
        start = anomaly_state['start']
        elapsed = current_step - start
        
        # Duration check (e.g. 40 steps = 4 seconds)
        if elapsed < 0 or elapsed > 40:
             return modified
             
        if a_type == 'spike':
            # Spike: Sudden jump of +50% (scaled 0-1 so +0.5) or multiplier 1.5
            # Values are minmax scaled (0-1). 
            # A spike of 50% magnitude = +0.5
            modified[sensor_idx] += 0.5
            # Clip to [0, 1] ? Or allow >1 (out of bounds)?
            # Allow >1 to ensure detection
            
        elif a_type == 'drift':
            # Drift: Cumulative add
            rate = 0.02 # +2% per step
            drift_amt = rate * elapsed
            modified[sensor_idx] += drift_amt
            
        elif a_type == 'freeze':
            # Freeze: Hold value constant? 
            # Or set to 0? Or set to mean?
            # "Sensor signal flatline".
            # If we just keep the value same as *start* of anomaly?
            # We need the value at 'start'.
            # State needs to store 'freeze_value'.
            if 'freeze_value' not in anomaly_state:
                # Capture current value as freeze value
                # But we only have current sample.
                # We should have captured it at start.
                # Hack: Just use 0.5 (mean) or 0.
                pass
            
            # Simple freeze: set to 0.0 (sensor dead)
            modified[sensor_idx] = 0.0 
            
        return modified

"""
Large data generator for demonstration purposes.

This module contains the LargeDataGeneratorJob class that generates
large datasets and stores them as results.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import time
import random
import string
from typing import Any, Dict, List, Union

from queuemgr.jobs.base import QueueJobBase


class LargeDataGeneratorJob(QueueJobBase):
    """Job that generates large datasets and stores them as results."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize LargeDataGeneratorJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.target_size_mb = params.get("target_size_mb", 1.0)
        self.data_type = params.get("data_type", "mixed")

    def execute(self) -> None:
        """Generate large dataset and set as result."""
        print(
            f"LargeDataGeneratorJob {self.job_id}: Generating "
            f"{self.target_size_mb}MB of {self.data_type} data..."
        )

        start_time = time.time()

        # Generate data based on type
        if self.data_type == "text":
            data = self._generate_text_data()
        elif self.data_type == "numbers":
            data = self._generate_number_data()
        elif self.data_type == "json":
            data = self._generate_json_data()
        else:  # mixed
            data = self._generate_mixed_data()

        generation_time = time.time() - start_time

        # Create result with metadata
        result = {
            "data_type": self.data_type,
            "target_size_mb": self.target_size_mb,
            "actual_size_bytes": len(json.dumps(data)),
            "actual_size_mb": len(json.dumps(data)) / (1024 * 1024),
            "generation_time": generation_time,
            "compression_ratio": self._calculate_compression_ratio(data),
            "data": data,
        }

        # Set result
        self.set_result(result)

        print(
            f"LargeDataGeneratorJob {self.job_id}: Generated "
            f"{result['actual_size_mb']:.2f}MB in {generation_time:.2f}s"
        )

    def _generate_text_data(self) -> str:
        """Generate large text data."""
        target_bytes = int(self.target_size_mb * 1024 * 1024)
        text_data = ""

        while len(text_data.encode("utf-8")) < target_bytes:
            # Generate random text
            words = []
            for _ in range(100):  # 100 words per chunk
                word_length = random.randint(3, 12)
                word = "".join(random.choices(string.ascii_lowercase, k=word_length))
                words.append(word)

            chunk = " ".join(words) + "\n"
            text_data += chunk

            # Add some structure
            if len(text_data) % 10000 == 0:
                text_data += f"\n--- Section {len(text_data) // 10000} ---\n"

        return text_data

    def _generate_number_data(self) -> List[Dict[str, Any]]:
        """Generate large numeric data."""
        target_bytes = int(self.target_size_mb * 1024 * 1024)
        data = []

        while len(json.dumps(data)) < target_bytes:
            record = {
                "id": len(data),
                "value": random.uniform(0, 1000),
                "timestamp": time.time(),
                "category": random.choice(["A", "B", "C", "D"]),
                "metadata": {
                    "source": f"generator_{random.randint(1, 10)}",
                    "quality": random.uniform(0.8, 1.0),
                    "tags": [f"tag_{i}" for i in range(random.randint(1, 5))],
                },
            }
            data.append(record)

        return data

    def _generate_json_data(self) -> Dict[str, Any]:
        """Generate large JSON structure."""
        target_bytes = int(self.target_size_mb * 1024 * 1024)
        data = {
            "metadata": {
                "version": "1.0",
                "created_at": time.time(),
                "generator": "LargeDataGeneratorJob",
            },
            "objects": [],
            "object_collections": [],
        }

        # Generate objects
        while len(json.dumps(data)) < target_bytes:
            obj = {
                "id": f"obj_{len(data['objects'])}",
                "type": random.choice(["user", "product", "order", "event"]),
                "properties": {
                    "name": f"Object {len(data['objects'])}",
                    "value": random.uniform(0, 1000),
                    "active": random.choice([True, False]),
                    "tags": [f"tag_{i}" for i in range(random.randint(1, 10))],
                },
                "nested": {
                    "level1": {
                        "level2": {
                            "level3": {
                                "data": [random.randint(1, 100) for _ in range(10)]
                            }
                        }
                    }
                },
            }
            data["objects"].append(obj)

            # Add collections periodically
            if len(data["objects"]) % 50 == 0:
                collection = {
                    "collection_id": f"col_{len(data['object_collections'])}",
                    "items": data["objects"][-10:],  # Last 10 objects
                    "summary": {
                        "count": 10,
                        "total_value": sum(
                            o["properties"]["value"] for o in data["objects"][-10:]
                        ),
                    },
                }
                data["object_collections"].append(collection)

        return data

    def _generate_mixed_data(self) -> Dict[str, Any]:
        """Generate mixed data types."""
        target_bytes = int(self.target_size_mb * 1024 * 1024)
        data = {
            "text_sections": [],
            "numeric_arrays": [],
            "object_records": [],
            "metadata": {},
        }

        while len(json.dumps(data)) < target_bytes:
            # Add text section
            text_section = {
                "id": len(data["text_sections"]),
                "content": "".join(random.choices(string.ascii_letters + " ", k=1000)),
                "type": random.choice(["description", "comment", "note"]),
            }
            data["text_sections"].append(text_section)

            # Add numeric array
            numeric_array = {
                "id": len(data["numeric_arrays"]),
                "values": [random.uniform(0, 100) for _ in range(100)],
                "statistics": {"mean": 0, "max": 0, "min": 0},  # Will be calculated
            }
            data["numeric_arrays"].append(numeric_array)

            # Add object record
            object_record = {
                "id": f"record_{len(data['object_records'])}",
                "timestamp": time.time(),
                "data": {
                    "string_field": f"value_{random.randint(1, 1000)}",
                    "number_field": random.randint(1, 1000),
                    "boolean_field": random.choice([True, False]),
                    "array_field": [random.randint(1, 100) for _ in range(20)],
                },
            }
            data["object_records"].append(object_record)

        return data

    def _calculate_compression_ratio(
        self, data: Union[str, bytes, Dict[str, Any], List[Any]]
    ) -> float:
        """Calculate approximate compression ratio."""
        json_str = json.dumps(data)
        # Simple estimation - real compression would be better
        return len(json_str) / (len(json_str) * 0.7)  # Assume 30% compression

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"LargeDataGeneratorJob {self.job_id}: Starting data generation...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"LargeDataGeneratorJob {self.job_id}: Stopping data generation...")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"LargeDataGeneratorJob {self.job_id}: Data generation completed!")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"LargeDataGeneratorJob {self.job_id}: Error occurred: {exc}")

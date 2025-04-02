"""
Data models for trucking tasks and related entities.
"""
from typing import Dict, List, Optional, Any
from datetime import date
from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    """Type of task: pickup (from) or delivery (to)."""
    PICKUP = "pickup"
    DELIVERY = "delivery"


@dataclass
class TaskLocation:
    """Represents a location for a task (pickup or delivery)."""
    local_id: str
    location_name: Optional[str] = None
    location_address: str = ""
    logist_comment: Optional[str] = None
    action_tag: Optional[str] = None
    action_tag_subtype: Optional[str] = None
    parcel_weight: Optional[float] = None
    task_date: Optional[date] = None
    time_comment: Optional[str] = None
    refuel_volume: Optional[float] = None
    refuel_full_tank: Optional[bool] = None
    adblue_volume: Optional[float] = None
    adblue_full_tank: Optional[bool] = None
    temperature_info: Optional[str] = None
    driver_atch_tags: Optional[str] = None
    driver_atch_tags_visit_disabled: Optional[bool] = None
    # Coordinates will be populated by address validation
    lat: Optional[float] = None
    lng: Optional[float] = None
    # Database tracking
    task_id: Optional[str] = None
    address_id: Optional[int] = None
    # Task type (pickup or delivery)
    task_type: TaskType = TaskType.PICKUP
    # Optional related task ID (for linking pickup to delivery if needed)
    related_task_id: Optional[str] = None
    # Sequence number for ordering tasks in a route
    sequence: int = 0

    def to_api_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for API."""
        result = {
            "localId": self.local_id,
            "locationAddress": self.location_address,
        }

        # Only include non-None values
        if self.location_name:
            result["locationName"] = self.location_name
        if self.logist_comment:
            result["logistComment"] = self.logist_comment
        if self.action_tag:
            result["actionTag"] = self.action_tag
        if self.action_tag_subtype:
            result["actionTagSubtype"] = self.action_tag_subtype
        if self.parcel_weight is not None:
            result["parcelWeight"] = self.parcel_weight
        if self.task_date:
            result["date"] = self.task_date.strftime("%Y%m%d")
        if self.time_comment:
            result["timeComment"] = self.time_comment
        if self.refuel_volume is not None:
            result["refuelVolume"] = self.refuel_volume
        if self.refuel_full_tank is not None:
            result["refuelFullTank"] = self.refuel_full_tank
        if self.adblue_volume is not None:
            result["adblueVolume"] = self.adblue_volume
        if self.adblue_full_tank is not None:
            result["adblueFullTank"] = self.adblue_full_tank
        if self.temperature_info:
            result["temperatureInfo"] = self.temperature_info
        if self.driver_atch_tags:
            result["driverAtchTags"] = self.driver_atch_tags
        if self.driver_atch_tags_visit_disabled is not None:
            result["driverAtchTagsVisitDisabled"] = self.driver_atch_tags_visit_disabled

        # Add coordinates if they're populated
        if self.lat is not None and self.lng is not None:
            result["lat"] = self.lat
            result["lng"] = self.lng

        return result

    @classmethod
    def from_pdf_order(cls, order_data: Dict[str, str], is_pickup: bool, local_id_prefix: str) -> 'TaskLocation':
        """Create a TaskLocation from PDF extracted order data."""
        key = "load" if is_pickup else "unload"
        address = order_data.get(key, "")

        # Clean address by removing newlines and extra spaces
        clean_address = " ".join(address.replace("\n", " ").split())

        # Generate a unique local ID
        local_id = f"{local_id_prefix}-{'LOAD' if is_pickup else 'UNLOAD'}"

        # Set default action tag based on load/unload
        action_tag = "PARCEL_LOAD" if is_pickup else "PARCEL_UNLOAD"

        return cls(
            local_id=local_id,
            location_address=clean_address,
            action_tag=action_tag,
            task_type=TaskType.PICKUP if is_pickup else TaskType.DELIVERY
        )


@dataclass
class RouteTask:
    """A task in a route sequence."""
    task: TaskLocation
    device_number: str


@dataclass
class Route:
    """Represents a complete route with tasks in sequence."""
    tasks: List[RouteTask]
    device_number: str

    def add_task(self, task: TaskLocation) -> None:
        """Add a task to the route."""
        self.tasks.append(RouteTask(task=task, device_number=self.device_number))

    def get_locations(self) -> List[TaskLocation]:
        """Get all task locations in this route."""
        return [task.task for task in self.tasks]


@dataclass
class ProcessingResult:
    """Result of task processing for reporting."""
    task_id: str
    address: str
    is_verified: bool
    original_lat: Optional[float] = None
    original_lng: Optional[float] = None
    verified_lat: Optional[float] = None
    verified_lng: Optional[float] = None
    task_type: Optional[TaskType] = None
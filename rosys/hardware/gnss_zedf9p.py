from __future__ import annotations

import logging
import math
import serial
from datetime import UTC, datetime
from enum import IntEnum

from serial.tools import list_ports
from pyubx2 import UBXReader, UBXMessage, GET

from .. import rosys
from ..driving.driver import PoseProvider
from ..event import Event
from ..geometry import GeoPoint, GeoPose, Pose
from ..run import io_bound

from .gnss import Gnss, GnssMeasurement, GpsQuality


class GnssZEDF9P(Gnss):
    """This hardware module connects to a SparkFun GPS RTK2 Board (ZED-F9P) GNSS receiver."""

    # Maximum allowed timestamp difference in seconds
    MAX_TIMESTAMP_DIFF = 0.05
    HISTORY_SAMPLE_PERIOD_S = 1
    TOTAL_HISTORY_SAMPLE_DURATION_S = 120

    def __init__(self, *, antenna_pose: Pose | None, reconnect_interval: float = 3.0) -> None:
        self.sample_history = []
        """
        :param antenna_pose: the pose of the main antenna in the robot's coordinate frame (yaw: direction to the auxiliary antenna)
        :param reconnect_interval: the interval to wait before reconnecting to the device
        """
        super().__init__()
        self.antenna_pose = antenna_pose or Pose(x=0.0, y=0.0, yaw=0.0)
        self._reconnect_interval = reconnect_interval
        self.serial_connection: serial.Serial | None = None
        self.ubx_reader: UBXReader | None = None
        rosys.on_startup(self._run)

    @property
    def is_connected(self) -> bool:
        if self.serial_connection is None:
            self.log.debug('Device not connected')
            return False
        if not self.serial_connection.isOpen():
            self.log.debug('Device not open')
            return False
        return True

    async def _run(self) -> None:
        while True:
            if not self.is_connected:
                try:
                    serial_device_path = self._find_device()
                    self.serial_connection = self._connect_to_device(serial_device_path)
                    self.ubx_reader = UBXReader(self.serial_connection)
                except RuntimeError:
                    self.log.error('Could not connect to ZED-F9P device: %s', serial_device_path)
                    await rosys.sleep(self._reconnect_interval)
                    continue
                self.log.info('Connected to ZED-F9P device: %s', serial_device_path)
                # NOTE: Allow time for the device to stabilize after connection
                await rosys.sleep(0.1)
                assert self.serial_connection is not None
                self.serial_connection.reset_input_buffer()
                await self._configure_zed_f9p()

            assert self.ubx_reader is not None
            try:
                (raw_msg, parsed_msg) = await io_bound(self.ubx_reader.read)
                if parsed_msg is not None and parsed_msg.identity == 'NAV-PVT':
                    self._process_nav_pvt_message(parsed_msg)
            except Exception as e:
                self.log.exception('Error reading/parsing UBX message: %s', e)
                await rosys.sleep(0.1) # Avoid busy-waiting on errors

    async def _configure_zed_f9p(self) -> None:
        assert self.serial_connection is not None

        # Disable NMEA on UART1 (if enabled) and enable UBX
        cfg_prt = UBXMessage('CFG', 'CFG-PRT', portID=1, inProtoMask=1, outProtoMask=2)
        self.serial_connection.write(cfg_prt.serialize())
        self.log.info('Configured ZED-F9P UART1 for UBX protocol.')

        # Set NAV-PVT message rate to 20Hz
        cfg_msg = UBXMessage('CFG', 'CFG-MSG', msgClass=0x01, msgID=0x07, rate=20)
        self.serial_connection.write(cfg_msg.serialize())
        self.log.info('Configured ZED-F9P to output NAV-PVT at 20Hz.')

        # Save configuration
        cfg_cfg = UBXMessage('CFG', 'CFG-CFG', clearMask=0, saveMask=0xFFFF, loadMask=0)
        self.serial_connection.write(cfg_cfg.serialize())
        self.log.info('Saved ZED-F9P configuration.')

    def _process_nav_pvt_message(self, msg) -> None:
        rosys_time = rosys.time()
        gnss_time = datetime(msg.year, msg.month, msg.day, msg.hour, msg.minute, msg.second, tzinfo=UTC).timestamp()

        # Check for valid fix type (3D fix, DGPS, RTK fixed/float)
        if msg.fixType < 3:
            self.log.debug('No valid 3D fix (fixType: %s)', msg.fixType)
            return

        # Convert ECEF coordinates to GeoPose (latitude, longitude, heading)
        # ZED-F9P provides lat/lon directly in NAV-PVT
        latitude = math.radians(msg.lat / 1e7)  # degrees to radians
        longitude = math.radians(msg.lon / 1e7)  # degrees to radians
        heading = math.radians(msg.headMot / 1e5) if msg.headMot != 0 else 0.0 # degrees to radians

        # Accuracy estimates (convert to meters and degrees)
        latitude_std_dev = msg.hAcc / 1e3  # mm to meters
        longitude_std_dev = msg.hAcc / 1e3  # mm to meters (assuming hAcc applies to both)
        heading_std_dev = msg.headAcc / 1e5  # degrees * 1e-5 to degrees

        # GPS Quality mapping
        gps_quality = GpsQuality.INVALID
        if msg.fixType == 3: # 3D fix
            if msg.flags & 0x01: # gnssFixOK
                if msg.flags & 0x02: # diffSoln
                    if msg.flags & 0x04: # WLS
                        gps_quality = GpsQuality.RTK_FIXED # Assuming WLS implies RTK Fixed
                    else:
                        gps_quality = GpsQuality.RTK_FLOAT # Assuming diffSoln without WLS implies RTK Float
                else:
                    gps_quality = GpsQuality.GPS
        elif msg.fixType == 4: # GNSS + Dead Reckoning
            gps_quality = GpsQuality.DEAD_RECKONING
        elif msg.fixType == 5: # Time Only Fix
            gps_quality = GpsQuality.PPS
        elif msg.fixType == 6: # Dead Reckoning Only
            gps_quality = GpsQuality.DEAD_RECKONING

        num_satellites = msg.numSV
        hdop = msg.pDOP / 100.0 if msg.pDOP != 0 else 0.0 # 0.01
        altitude = msg.hMSL / 1e3 # mm to meters

        last_heading = heading + self.antenna_pose.yaw
        antenna_pose = GeoPose(lat=latitude, lon=longitude, heading=last_heading)
        robot_pose = antenna_pose.relative_shift_by(x=-self.antenna_pose.x, y=-self.antenna_pose.y)

        self.last_measurement = GnssMeasurement(
            time=rosys_time,
            gnss_time=gnss_time,
            pose=robot_pose,
            latitude_std_dev=latitude_std_dev,
            longitude_std_dev=longitude_std_dev,
            heading_std_dev=heading_std_dev,
            gps_quality=gps_quality,
            num_satellites=num_satellites,
            hdop=hdop,
            altitude=altitude,
        )
        self.NEW_MEASUREMENT.emit(self.last_measurement)
        self.update_sample_history(self.last_measurement)


    def _find_device(self) -> str:
        for port in list_ports.comports():
            self.log.debug('Found port: %s - %s', port.device, port.description)
            # ZED-F9P often enumerates as a u-blox device
            if 'u-blox' in port.description or 'ZED-F9P' in port.description:
                self.log.debug('Found ZED-F9P device: %s', port.device)
                return port.device
        raise RuntimeError('No ZED-F9P device found')

    def _connect_to_device(self, port: str, *, baudrate: int = 115200, timeout: float = 0.2) -> serial.Serial:
        self.log.debug('Connecting to ZED-F9P device "%s"...', port)
        try:
            return serial.Serial(port=port, baudrate=baudrate, timeout=timeout)
        except serial.SerialException as e:
            raise RuntimeError(f'Could not connect to ZED-F9P device: {port}') from e




    def update_sample_history(self, latest_sample: GnssMeasurement):
        """
        Save samples in to a buffer periodically, for GPS drift analysis.
        """
        if not latest_sample.gps_quality in [GpsQuality.RTK_FIXED, GpsQuality.RTK_FLOAT, GpsQuality.GPS]:
            return
        
        current_time = rosys.time()
        if not self.sample_history or current_time - self.sample_history[-1][0] > self.HISTORY_SAMPLE_PERIOD_S:
            self.sample_history.append((current_time, latest_sample))
        
        while self.sample_history and current_time - self.sample_history[0][0] > self.TOTAL_HISTORY_SAMPLE_DURATION_S:
            self.sample_history.pop(0)




    def print_sample_history_stats(self):
        if len(self.sample_history) < 2:
            return
        oldest_sample = self.sample_history[0][1]
        newest_sample = self.sample_history[-1][1]
        data_duration = newest_sample.time - oldest_sample.time
        # NOTE: Assuming GeoPoint has a distance method or similar utility is available
        # If not, a helper function would be needed, similar to gps_tools.get_distance
        # For now, let's assume a utility to calculate distance between GeoPoints exists or can be added.
        # For simplicity, we'll just log the coordinates difference for now.
                # To calculate distance, we need a utility function. 
        
        self.log.info(f"GPS history - oldest: (lat={oldest_sample.pose.lat:.8f}, lon={oldest_sample.pose.lon:.8f}), newest: (lat={newest_sample.pose.lat:.8f}, lon={newest_sample.pose.lon:.8f}), total time: {data_duration:.3f}, {len(self.sample_history)} samples")
        # If a distance calculation utility is available, uncomment and use the following:
        # distance_oldest_newest = rosys.geometry.get_distance(newest_sample.pose, oldest_sample.pose)
        # max_distance = 0
        # for sample in self.sample_history:
        #     distance = rosys.geometry.get_distance(newest_sample.pose, sample[1].pose)
        #     if distance > max_distance:
        #         max_distance = distance
        # self.log.info(f"GPS history - oldest-newest distance: {distance_oldest_newest:.3f}, max distance in samples: {max_distance:.3f}, total time: {data_duration:.3f}, {len(self.sample_history)} samples")


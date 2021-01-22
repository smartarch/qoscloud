"""
Contains the classes responsible for user equipment management.
"""
import logging
from typing import List, Dict, Iterable, Optional
from enum import Enum

import cloud_controller.knowledge.knowledge_pb2 as protocols


class UEManagementPolicy(Enum):
    FREE = 1
    WHITELIST = 2


class UserEquipment:
    """
    Represents a single user equipment. Is identified by its IP address. Thus, there cannot be two UE in the system
    with the same IP address.

    Attributes:
        ip:             UE IP address.
        imsi:           Specific ID of the UE.
        association:    A network element to which this UE is connected. By default means the closest datacenter, but
                        can mean different things for different network topologies (e.g. eNodeB, SPGW-U, etc.)
        apps:           A list of applications this UE is subscribed to. Is relevant only for applications with
                        WHITELIST UEManagementPolicy. From the point of view of the Avocado framework, these
                        applications have to be written in form "application-name.component-name"
    """

    def __init__(self, ip: str, imsi: str = None, association: str = None, apps: List[str] = None):
        """
        :param ip: UE IP address.
        :param imsi: Specific ID of the UE.
        :param association: A network element to which this UE is connected.
        :param apps: A list of applications this UE is subscribed to.
        """
        if apps is None:
            apps = []
        self.ip = ip
        self.imsi = imsi
        self.association = association
        self.apps = apps


class UserEquipmentContainer:
    """
    Manages the instances of UserEquipment.

    Attributes:
        _user_equipment:            Collection of UserEquipment, mapped by UE IP
        _new_user_equipment:        List of all the user equipment that was added or changed since the last call of
                                    list_new_ue
        _removed_user_equipment:    List of all the user equipment that was removed since the last call of
                                    list_removed_ue
    """

    def __init__(self):
        self._user_equipment: Dict[str, UserEquipment] = {}
        self._new_user_equipment: List[protocols.UserEquipment] = []
        self._removed_user_equipment: List[protocols.UserEquipment] = []

    def add_ue(self, ue: UserEquipment, skip_cc_cache: bool = False) -> None:
        """
        Adds new UE to the container. If there is already a UE with that IP in the container, replaces it.
        :param ue: UE to add.
        :param skip_cc_cache: Set this to True if you want not to add this UE to the _new_user_equipment.
        """
        self._user_equipment[ue.ip] = ue
        if not skip_cc_cache:
            ue_pb = protocols.UserEquipment(ip=ue.ip, imsi=ue.imsi)
            for app_name in ue.apps:
                ue_pb.apps.append(app_name)
            self._new_user_equipment.append(ue_pb)

    def add_app_to_ue(self, ue_ip: str, app: str) -> None:
        """
        Adds a new application subscription to the UE. If the UE does not exist, creates a new one.
        :param ue_ip: IP address of the UE
        :param app: Application to add. From the point of view of the Avocado framework, the name has to be written in
                    form "application-name.component-name"
        """
        if self.ue_registered(ue_ip):
            ue = self.get_ue(ue_ip)
            ue.apps.append(str)
            self.add_ue(ue)
        else:
            self.add_ue(UserEquipment(ip=ue_ip, apps=[app]))

    def remove_ue(self, ip: str) -> None:
        """
        Removes UE with the specified IP from the container
        :param ip: UE IP address
        """
        imsi = self._user_equipment[ip].imsi
        self._removed_user_equipment.append(protocols.UserEquipment(ip=ip, imsi=imsi))
        del self._user_equipment[ip]

    def process_network_handover(self, ip: str, new_association: str) -> None:
        """
        Sets new network association for the given UE.
        :param ip: UE IP address
        :param new_association: An ID of the network element to which the UE will be associated.
        """
        ue = self._user_equipment[ip]
        ue.association = new_association

    @staticmethod
    def report_handover(ue_ip, application, component, old_association, new_association) -> None:
        """
        A hook method that is called when all the dependencies of the client were finally moved to the data center
        closest to the new_association. The implementation of this method can be set in the ExtensionManager. May be
        useful for reporting "handover complete" to a Network Controller.
        :param ue_ip: UE IP address.
        :param application: Application for which the handover was completed.
        :param component: Component for which the handover was completed.
        :param old_association: Old network association (before handover)
        :param new_association: New network association (after handover)
        :return:
        """
        logging.info(f"Client {component} of application {application} with IP {ue_ip} is now using services located"
                     f"at {new_association} datacenter")

    def get_ue(self, ip: str) -> Optional[UserEquipment]:
        """
        :return: UE with the specified IP if exists, None otherwise.
        """
        if not self.ue_registered(ip):
            return None
        return self._user_equipment[ip]

    def ue_registered(self, ip: str) -> bool:
        """
        :return: True if there is a UE with the given IP, False otherwise
        """
        return ip in self._user_equipment

    def list_new_ue(self) -> Iterable[protocols.UserEquipment]:
        """
        :return: Iterator to all user equipment that was added to the container or changed since the last call of the
                    method. Will also include the UE that was subsequently removed. The UE is returned in protobuf
                    representation.
        """
        while len(self._new_user_equipment) > 0:
            ue = self._new_user_equipment.pop()
            yield ue

    def list_removed_ue(self) -> Iterable[protocols.UserEquipment]:
        """
        :return: Iterator to all user equipment that was removed from the container since the last call of the
                    method. Will also include the UE that was subsequently re-added. The UE is returned in protobuf
                    representation.
        """
        while len(self._removed_user_equipment) > 0:
            ue = self._removed_user_equipment.pop()
            yield ue

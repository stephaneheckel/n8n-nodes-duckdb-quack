import type { Icon } from "n8n-workflow";
import { ICredentialType, INodeProperties } from "n8n-workflow";
export declare class DuckDbQuackApi implements ICredentialType {
    name: string;
    displayName: string;
    documentationUrl: string;
    icon: Icon;
    properties: INodeProperties[];
}

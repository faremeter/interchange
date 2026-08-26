export {
  assembleSignedContent,
  assembleMessage,
  extractAddrSpec,
  formatRFC2822Date,
  generateMessageId,
  parseHeaderSection,
  parseMimePart,
  parseMultipart,
  extractBoundary,
  extractPartByPath,
  parseMailToEmail,
  extractAttachments,
  buildMessageHeaders,
  decodeMail,
} from "./mime";

export type {
  MessageHeaders,
  ConversationContent,
  MimeAssemblyInput,
  StructuredContent,
  ParsedMimePart,
  ParsedMimeMessage,
  JMAPEmail,
  JMAPAddress,
  JMAPBodyValue,
  JMAPBodyPart,
  JMAPAttachment,
} from "./mime";

export { createDetachedSignatureFromProvider } from "./pgp-sign";

export {
  createInboundMessage,
  createOutboundMessage,
  isMessageId,
} from "./mail-builder";

export type {
  CreateInboundMessageOpts,
  CreateOutboundMessageOpts,
  InboundPayloadInput,
} from "./mail-builder";

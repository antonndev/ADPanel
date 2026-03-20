




const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const BRANDING_CONFIG_PATH = path.join(__dirname, "..", "data", "branding.json");


let brandingCache = null;
let brandingCacheTime = 0;
const CACHE_TTL = 60000; 


const DEFAULT_BRANDING = {
  appName: "ADPanel",
  logoUrl: null,
  localLogoPath: null,
  loginWatermarkUrl: "https://stalwart-pegasus-2c2ca4.netlify.app/watermark.webp",
  localLoginWatermarkPath: null,
  loginBackgroundType: "video",
  loginBackgroundExternalUrl: null,
  localLoginBackgroundPath: null,
  loginBackgroundMimeType: "video/webm"
};


const ALLOWED_EXTENSIONS = ["png", "jpg", "jpeg", "webp", "ico", "svg"];
const ALLOWED_VIDEO_EXTENSIONS = ["webm", "mp4", "ogg", "ogv"];
const DEFAULT_LOGIN_BACKGROUND_URL = "/images/bgvid.webm";
const PUBLIC_IMAGES_DIR = path.join(__dirname, "..", "public", "images");


const IMAGE_MAGIC_BYTES = {
  png: [0x89, 0x50, 0x4E, 0x47],
  jpg: [0xFF, 0xD8, 0xFF],
  jpeg: [0xFF, 0xD8, 0xFF],
  webp: [0x52, 0x49, 0x46, 0x46], 
  ico: [0x00, 0x00, 0x01, 0x00],
  svg: null 
};

const VIDEO_MIME_TYPES = {
  webm: "video/webm",
  mp4: "video/mp4",
  ogg: "video/ogg",
  ogv: "video/ogg"
};

// Prevent path traversal in local asset filenames
function safeBasename(p) {
  if (!p || typeof p !== 'string') return null;
  const b = path.basename(p);
  return (b && b !== '.' && b !== '..') ? b : null;
}

function getBrandingAssetVersionSuffix(branding) {
  const updatedAt = typeof branding?.updatedAt === "string" ? branding.updatedAt.trim() : "";
  return updatedAt ? `?v=${encodeURIComponent(updatedAt)}` : "";
}

function resolveLocalBrandingAssetUrl(filename, branding) {
  const safe = safeBasename(filename);
  if (!safe) return null;

  const assetPath = path.join(PUBLIC_IMAGES_DIR, safe);
  if (!fs.existsSync(assetPath)) return null;

  return `/images/${safe}${getBrandingAssetVersionSuffix(branding)}`;
}

function resolveLoginWatermarkAssetUrl(branding) {
  const config = branding && typeof branding === "object" ? branding : loadBrandingConfig();
  const localUrl = resolveLocalBrandingAssetUrl(config.localLoginWatermarkPath, config);
  if (localUrl) return localUrl;
  if (config.loginWatermarkUrl) {
    return `/branding-media/login-watermark${getBrandingAssetVersionSuffix(config)}`;
  }
  return DEFAULT_BRANDING.loginWatermarkUrl;
}

function resolveLoginBackgroundAssetUrl(branding) {
  const config = branding && typeof branding === "object" ? branding : loadBrandingConfig();
  const localUrl = resolveLocalBrandingAssetUrl(config.localLoginBackgroundPath, config);
  if (localUrl) return localUrl;
  if (config.loginBackgroundExternalUrl) {
    return `/branding-media/login-background${getBrandingAssetVersionSuffix(config)}`;
  }
  return DEFAULT_LOGIN_BACKGROUND_URL;
}





function loadBrandingConfig() {
  const now = Date.now();
  
  
  if (brandingCache && (now - brandingCacheTime) < CACHE_TTL) {
    return brandingCache;
  }
  
  try {
    if (fs.existsSync(BRANDING_CONFIG_PATH)) {
      const data = fs.readFileSync(BRANDING_CONFIG_PATH, "utf8");
      const config = JSON.parse(data);
      
      
      brandingCache = {
        ...DEFAULT_BRANDING,
        ...config
      };
      brandingCacheTime = now;
      return brandingCache;
    }
  } catch (err) {
    console.error("[branding] Error loading config:", err.message);
  }
  
  
  brandingCache = { ...DEFAULT_BRANDING };
  brandingCacheTime = now;
  return brandingCache;
}






function saveBrandingConfig(config) {
  try {
    
    const dataDir = path.dirname(BRANDING_CONFIG_PATH);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    
    
    const existing = loadBrandingConfig();
    const newConfig = {
      ...existing,
      ...config,
      updatedAt: new Date().toISOString()
    };
    
    
    const tempPath = BRANDING_CONFIG_PATH + ".tmp";
    fs.writeFileSync(tempPath, JSON.stringify(newConfig, null, 2), { mode: 0o600 });
    fs.renameSync(tempPath, BRANDING_CONFIG_PATH);
    
    
    brandingCache = null;
    brandingCacheTime = 0;
    
    return true;
  } catch (err) {
    console.error("[branding] Error saving config:", err.message);
    return false;
  }
}






function generateSafeLogoFilename(extension) {
  const randomId = crypto.randomBytes(8).toString("hex");
  return `logo-${randomId}.${extension}`;
}






function validateBase64Image(base64Data) {
  try {
    
    let cleanBase64 = base64Data;
    if (base64Data.includes(",")) {
      cleanBase64 = base64Data.split(",")[1];
    }
    
    
    const buffer = Buffer.from(cleanBase64, "base64");
    
    
    if (buffer.length < 8) {
      return { valid: false, error: "Image data too small" };
    }
    
    
    if (buffer.length > 5 * 1024 * 1024) {
      return { valid: false, error: "Image too large (max 5MB)" };
    }
    
    
    let validType = false;
    for (const [type, magic] of Object.entries(IMAGE_MAGIC_BYTES)) {
      if (magic === null) continue; 
      
      let matches = true;
      for (let i = 0; i < magic.length; i++) {
        if (buffer[i] !== magic[i]) {
          matches = false;
          break;
        }
      }
      if (matches) {
        validType = true;
        break;
      }
    }
    
    
    if (!validType) {
      const text = buffer.toString("utf8", 0, Math.min(100, buffer.length)).toLowerCase();
      if (text.includes("<svg") || text.includes("<?xml")) {
        
        const fullText = buffer.toString("utf8").toLowerCase();
        if (fullText.includes("<script") || fullText.includes("javascript:") || /\bon[a-z]+\s*=/.test(fullText)) {
          return { valid: false, error: "SVG contains potentially dangerous content" };
        }
        validType = true;
      }
    }
    
    if (!validType) {
      return { valid: false, error: "Unrecognized image format" };
    }
    
    return { valid: true, data: buffer };
  } catch (err) {
    return { valid: false, error: "Failed to decode image data" };
  }
}






function getValidatedExtension(filename) {
  if (!filename || typeof filename !== "string") {
    return null;
  }
  
  
  const parts = filename.toLowerCase().split(".");
  if (parts.length < 2) {
    return null;
  }
  
  const ext = parts[parts.length - 1].replace(/[^a-z0-9]/g, "");
  
  if (ALLOWED_EXTENSIONS.includes(ext)) {
    return ext;
  }
  
  return null;
}

function getValidatedVideoExtension(filename) {
  if (!filename || typeof filename !== "string") {
    return null;
  }

  const parts = filename.toLowerCase().split(".");
  if (parts.length < 2) {
    return null;
  }

  const ext = parts[parts.length - 1].replace(/[^a-z0-9]/g, "");
  if (ALLOWED_VIDEO_EXTENSIONS.includes(ext)) {
    return ext;
  }

  return null;
}

function getMimeTypeForExtension(extension) {
  if (!extension || typeof extension !== "string") {
    return null;
  }

  const normalized = extension.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (normalized === "png") return "image/png";
  if (normalized === "jpg" || normalized === "jpeg") return "image/jpeg";
  if (normalized === "webp") return "image/webp";
  if (normalized === "ico") return "image/x-icon";
  if (normalized === "svg") return "image/svg+xml";
  return VIDEO_MIME_TYPES[normalized] || null;
}

function getMediaTypeFromFilename(filename) {
  if (getValidatedExtension(filename)) return "image";
  if (getValidatedVideoExtension(filename)) return "video";
  return null;
}

function getMediaTypeFromUrl(url) {
  if (!url || typeof url !== "string") {
    return null;
  }

  try {
    const parsed = new URL(url);
    const pathname = (parsed.pathname || "").toLowerCase();
    const ext = pathname.includes(".") ? pathname.slice(pathname.lastIndexOf(".") + 1) : "";
    if (!ext) return null;
    if (ALLOWED_EXTENSIONS.includes(ext)) return "image";
    if (ALLOWED_VIDEO_EXTENSIONS.includes(ext)) return "video";
  } catch (err) {
    return null;
  }

  return null;
}

function getExtensionFromUrl(url) {
  if (!url || typeof url !== "string") {
    return null;
  }

  try {
    const parsed = new URL(url);
    const pathname = (parsed.pathname || "").toLowerCase();
    if (!pathname.includes(".")) return null;
    const ext = pathname.slice(pathname.lastIndexOf(".") + 1).replace(/[^a-z0-9]/g, "");
    if (ALLOWED_EXTENSIONS.includes(ext) || ALLOWED_VIDEO_EXTENSIONS.includes(ext)) {
      return ext;
    }
  } catch (err) {
    return null;
  }

  return null;
}

function validateBase64Video(base64Data, expectedExt) {
  try {
    let cleanBase64 = base64Data;
    let declaredMime = "";
    if (typeof base64Data === "string" && base64Data.startsWith("data:")) {
      const match = base64Data.match(/^data:([^;,]+)[;,]/i);
      declaredMime = match ? String(match[1] || "").toLowerCase() : "";
    }
    if (typeof base64Data === "string" && base64Data.includes(",")) {
      cleanBase64 = base64Data.split(",")[1];
    }

    const buffer = Buffer.from(cleanBase64, "base64");
    if (buffer.length < 32) {
      return { valid: false, error: "Video data too small" };
    }
    if (buffer.length > 40 * 1024 * 1024) {
      return { valid: false, error: "Video too large (max 40MB)" };
    }

    const ext = String(expectedExt || "").toLowerCase();
    let valid = false;

    if (ext === "webm") {
      valid = buffer[0] === 0x1a && buffer[1] === 0x45 && buffer[2] === 0xdf && buffer[3] === 0xa3;
    } else if (ext === "mp4") {
      valid = buffer.slice(4, 8).toString("ascii") === "ftyp";
    } else if (ext === "ogg" || ext === "ogv") {
      valid = buffer.slice(0, 4).toString("ascii") === "OggS";
    }

    if (!valid) {
      return { valid: false, error: "Unrecognized video format" };
    }

    if (declaredMime && !declaredMime.startsWith("video/")) {
      return { valid: false, error: "Invalid video MIME type" };
    }

    return { valid: true, data: buffer };
  } catch (err) {
    return { valid: false, error: "Failed to decode video data" };
  }
}




function invalidateBrandingCache() {
  brandingCache = null;
  brandingCacheTime = 0;
}






function sanitizeAppName(appName) {
  if (!appName || typeof appName !== "string") {
    return null;
  }
  
  
  const trimmed = appName.trim().slice(0, 50);
  
  
  
  const sanitized = trimmed.replace(/[^a-zA-Z0-9\s\-_.!]/g, "");
  
  if (sanitized.length < 1) {
    return null;
  }
  
  return sanitized;
}






function isPrivateOrReservedHost(hostname) {
  if (!hostname) return true;
  const h = hostname.toLowerCase();

  if (h === "localhost" || h.endsWith(".localhost")) return true;

  const ipv4 = h.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipv4) {
    const [a, b] = [parseInt(ipv4[1], 10), parseInt(ipv4[2], 10)];
    if (a === 0) return true;                          // 0.0.0.0/8
    if (a === 10) return true;                         // 10.0.0.0/8
    if (a === 127) return true;                        // 127.0.0.0/8
    if (a === 169 && b === 254) return true;           // 169.254.0.0/16 (link-local / cloud metadata)
    if (a === 172 && b >= 16 && b <= 31) return true;  // 172.16.0.0/12
    if (a === 192 && b === 168) return true;           // 192.168.0.0/16
    return false;
  }

  // IPv6 (Node.js URL parser may keep brackets)
  const ipv6 = h.startsWith("[") && h.endsWith("]") ? h.slice(1, -1) : h;
  if (ipv6 === "::1" || ipv6 === "::" || ipv6.startsWith("fe80:") || ipv6.startsWith("fc") || ipv6.startsWith("fd")) return true;
  if (ipv6.startsWith("::ffff:")) {
    const mapped = ipv6.slice(7);
    // Dotted-decimal form (::ffff:127.0.0.1)
    if (mapped.includes(".")) return isPrivateOrReservedHost(mapped);
    // Hex form (::ffff:7f00:1) — convert to IPv4
    const hex = mapped.replace(":", "").padStart(8, "0");
    const a = parseInt(hex.slice(0, 2), 16), b = parseInt(hex.slice(2, 4), 16);
    const c = parseInt(hex.slice(4, 6), 16), d = parseInt(hex.slice(6, 8), 16);
    return isPrivateOrReservedHost(`${a}.${b}.${c}.${d}`);
  }

  return false;
}

function sanitizeLogoUrl(url) {
  if (!url || typeof url !== "string") {
    return null;
  }
  
  const trimmed = url.trim();
  
  
  if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
    return null;
  }
  
  
  try {
    const parsed = new URL(trimmed);
    
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }

    // Block requests to internal/private network addresses (SSRF protection)
    if (isPrivateOrReservedHost(parsed.hostname)) {
      return null;
    }

    return parsed.href;
  } catch (e) {
    return null;
  }
}







function brandingMiddleware(req, res, next) {
  const branding = loadBrandingConfig();
  const safeLogo = safeBasename(branding.localLogoPath);
  const safeWatermark = safeBasename(branding.localLoginWatermarkPath);
  
  
  res.locals.branding = {
    appName: branding.appName || DEFAULT_BRANDING.appName,
    logoUrl: safeLogo 
      ? `/images/${safeLogo}` 
      : (branding.logoUrl || null),
    loginWatermarkUrl: safeWatermark
      ? `/images/${safeWatermark}`
      : resolveLoginWatermarkAssetUrl(branding),
    loginBackgroundType: branding.loginBackgroundType || DEFAULT_BRANDING.loginBackgroundType,
    loginBackgroundUrl: resolveLoginBackgroundAssetUrl(branding),
    loginBackgroundMimeType: branding.loginBackgroundMimeType || DEFAULT_BRANDING.loginBackgroundMimeType
  };
  
  next();
}

module.exports = {
  brandingMiddleware,
  loadBrandingConfig,
  saveBrandingConfig,
  sanitizeAppName,
  sanitizeLogoUrl,
  generateSafeLogoFilename,
  validateBase64Image,
  validateBase64Video,
  getValidatedExtension,
  getValidatedVideoExtension,
  getMimeTypeForExtension,
  getMediaTypeFromFilename,
  getMediaTypeFromUrl,
  getExtensionFromUrl,
  getBrandingAssetVersionSuffix,
  resolveLoginWatermarkAssetUrl,
  resolveLoginBackgroundAssetUrl,
  invalidateBrandingCache,
  DEFAULT_BRANDING,
  DEFAULT_LOGIN_BACKGROUND_URL
};

module.exports = { makeCssBackground };

function makeCssBackground(type, value) {
  if (typeof value !== "string") return null;
  const v = value.trim();

  if (type === "color") {
    return /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(v) ? v : null;
  }

  if (type === "url" || type === "upload" || type === "image") {
    if (/^https?:\/\//i.test(v)) return `url("${v.replace(/["\\\n\r\f()<>]/g, encodeURIComponent)}")`;
    if (/^data:image\/(png|jpe?g|webp|gif);base64,[A-Za-z0-9+/=]+$/i.test(v)) return `url("${v}")`;
    if (/^\/[a-z0-9/_\-.]+$/i.test(v) && !v.includes('..')) return `url("${v}?v=${Date.now()}")`;
    return null;
  }

  return null;
}